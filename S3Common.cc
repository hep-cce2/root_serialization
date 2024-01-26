#include <atomic>
#include <cassert>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <thread>
#include <variant>
#include <random>
#include <sys/select.h>

#include "libs3.h"
#include <curl/curl.h>
#include "tbb/task_arena.h"
#include "tbb/concurrent_queue.h"
#include "S3Common.h"


namespace {
using namespace cce::tf;

class S3RequestWrapper {
  public:
    S3RequestWrapper(std::shared_ptr<S3Request> iReq, const S3BucketContext* iCtx, tbb::task_handle&& iCallback):
      req{std::move(iReq)}, bucketCtx{iCtx}, callback{std::move(iCallback)}
    {
      arena = std::make_unique<tbb::task_arena>(tbb::task_arena::attach{});
      backoffTimeout = req->timeout.count();
      submit_after = std::chrono::steady_clock::now();
    };

    void done() {
      arena->enqueue(std::move(callback));
    };

    std::shared_ptr<S3Request> req;
    const S3BucketContext* bucketCtx;
    tbb::task_handle callback;
    std::unique_ptr<tbb::task_arena> arena;
    size_t put_offset{0};
    int retries_executed{0};
    long backoffTimeout;
    std::chrono::steady_clock::time_point submit_after;
    static_assert(std::chrono::steady_clock::duration() <= std::chrono::milliseconds(1));
};

class S3LibWrapper {
  public:
    static S3LibWrapper& instance() {
      static S3LibWrapper instance;
      return instance;
    }
    S3LibWrapper(const S3LibWrapper&) = delete;
    void operator=(const S3LibWrapper&) = delete;

    bool running() const { return running_; }

    void submit(S3RequestWrapper* req) {
      requests_.push(req);
    }

  private:
    S3LibWrapper() : running_(false) {
      initStatus_ = S3_initialize("s3", S3_INIT_ALL, "");
      if ( initStatus_ != S3StatusOK ) {
        std::cerr << "Failed to initialize libs3, error: " << S3_get_status_name(initStatus_) << "\n";
        return;
      }
      running_ = true;
      loop_ = std::thread(&S3LibWrapper::loop_body, this);
    }

    ~S3LibWrapper() {
      running_ = false;
      if ( loop_.joinable() ) loop_.join();
      S3_deinitialize();
    }

    void loop_body() {
      S3RequestContext * ctx;
      fd_set read_fds, write_fds, except_fds;
      int max_fd, activeRequests{0};
      int topfds{0}, topreq{0};
      S3_create_request_context(&ctx);
      // For now we do not enable peer verification because CURL is loading the CA bundle per connection https://github.com/curl/curl/pull/9620
      // S3_set_request_context_verify_peer(ctx, 1);
      // auto status = curl_easy_setopt(curl, CURLOPT_CAPATH, "/etc/grid-security/certificates");
      // if ( status != CURLE_OK ) throw std::runtime_error("curle fail");
      std::vector<S3RequestWrapper*> to_defer;
      while(running_) {
        FD_ZERO(&read_fds);
        FD_ZERO(&write_fds);
        FD_ZERO(&except_fds);

        switch (S3_get_request_context_fdsets(ctx, &read_fds, &write_fds, &except_fds, &max_fd)) {
          case S3StatusOK:
            break;
          case S3StatusInternalError:
            throw std::runtime_error("internal error in S3_get_request_context_fdsets");
        }

        topfds = std::max(topfds, max_fd);

        if ( max_fd != -1 ) {
          int64_t timeout = std::min(100l, S3_get_request_context_timeout(ctx)); // milliseconds
          assert(timeout >= 0);
          struct timeval tv { timeout / 1000, (timeout % 1000) * 1000 };
          select(max_fd+1, &read_fds, &write_fds, &except_fds, &tv);
        }

        switch (S3_runonce_request_context(ctx, &activeRequests)) {
          case S3StatusOK:
            break;
          case S3StatusConnectionFailed:
            throw std::runtime_error("failed to connect in S3_runonce_request_context");
          case S3StatusServerFailedVerification:
            throw std::runtime_error("SSL verification failure in S3_runonce_request_context");
          case S3StatusInternalError:
            throw std::runtime_error("internal error in S3_runonce_request_context");
          case S3StatusOutOfMemory:
            throw std::runtime_error("out of memory while processing S3_runonce_request_context");
        }
        topreq = std::max(topreq, activeRequests);

        S3RequestWrapper* req;
        int currentlyActive{activeRequests};
        while (
            (activeRequests < asyncRequestLimit_)
            and activeRequests < (currentlyActive+asyncAddRequestLimit_)
            and requests_.try_pop(req) // test this last!
            ) {
          if ( req->submit_after <= std::chrono::steady_clock::now() ) {
            _submit(req, ctx);
            activeRequests++;
          } else {
            to_defer.push_back(req);
          }
        }
        for (auto req : to_defer) {
          requests_.push(req);
        }
        to_defer.clear();

        if ( activeRequests == 0 ) {
          // TODO: would be better to use a semaphore (submit() and ~S3LibWrapper need to notify)
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      }
      // TODO: this may abort requests in flight, do we wait or is it synchronous?
      S3_destroy_request_context(ctx);
      std::cout << "S3LibWrapper: max open file descriptors: " << topfds << ", max concurrent requests: " << topreq << std::endl;
    }

    void _submit(S3RequestWrapper* req, S3RequestContext* ctx) const {
      assert(ctx != nullptr);
      switch ( req->req->type ) {
        case S3Request::Type::undef:
          assert(false); // logic error
          break;
        case S3Request::Type::get:
          S3_get_object(
              req->bucketCtx,
              req->req->key.c_str(),
              nullptr, // S3GetConditions
              0, // startByte
              0, // byteCount
              ctx,
              req->backoffTimeout,
              &S3LibWrapper::getObjectHandler,
              static_cast<void*>(req));
          break;
        case S3Request::Type::put:
          S3_put_object(
              req->bucketCtx,
              req->req->key.c_str(),
              req->req->buffer.size(),
              nullptr, // S3PutProperties (TODO probably want .md5)
              ctx,
              req->backoffTimeout,
              &S3LibWrapper::putObjectHandler,
              static_cast<void*>(req));
          break;
      }
    }

    static S3Status responsePropertiesCallback(const S3ResponseProperties *properties, void *callbackData) {
      auto req = static_cast<S3RequestWrapper*>(callbackData);
      if ( req->req->type == S3Request::Type::get ) {
        if ( properties->contentLength > 0 ) {
          req->req->buffer.reserve(properties->contentLength);
        }
        // else what?
        // TODO: save headers?
      }
      return S3StatusOK;
      // perhaps S3StatusAbortedByCallback
    }

    static void responseCompleteCallback(S3Status status, const S3ErrorDetails *error, void *callbackData) {
      auto req = static_cast<S3RequestWrapper*>(callbackData);
      auto now = std::chrono::steady_clock::now();
      if ( S3_status_is_retryable(status) && req->retries_executed < req->req->retries ) {
        // e.g. S3StatusErrorRequestTimeout or ErrorSlowDown
        // Run backoff algo, https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        static thread_local std::minstd_rand rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
        std::uniform_int_distribution dist(0l, std::min(S3Request::max_timeout.count(), req->backoffTimeout));
        const auto dt = std::chrono::milliseconds(dist(rng));
        std::cerr << "Got status " << S3_get_status_name(status) << " while running request " 
          << *(req->req) << ", will retry in " << dt.count() << "ms\n";
        req->submit_after = now + dt;
        req->put_offset = 0;
        req->retries_executed++;
        req->backoffTimeout *= 2;
        instance().requests_.push(req);
        return; // no delete!
      }
      switch ( status ) {
        case S3StatusOK:
          req->req->status = S3Request::Status::ok;
          std::cerr << ((req->req->type == S3Request::Type::get) ? "get: " : "put: ")
            + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(now - req->submit_after).count())
            + " " + std::to_string(req->req->buffer.size())
            + " " + std::to_string(std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1))
            + "\n";
          break;
        default:
          std::cerr << "Got status " << S3_get_status_name(status) << " at end request " << *(req->req) << "\n";
          req->req->status = S3Request::Status::error;
      }
      req->done();
      // end of S3RequestWrapper lifecycle
      delete req;
    }

    static int putObjectDataCallback(int bufferSize, char *buffer, void *callbackData) {
      auto req = static_cast<S3RequestWrapper*>(callbackData);
      int toWrite = std::min(bufferSize, (int) (req->req->buffer.size() - req->put_offset));
      assert(toWrite >= 0);
      if ( toWrite > 0 ) {
        std::copy_n(req->req->buffer.begin() + req->put_offset, toWrite, buffer);
        req->put_offset += toWrite;
      }
      // return > 0 = bytes written, 0 = done, -1 = S3StatusAbortedByCallback
      return toWrite;
    }

    static S3Status getObjectDataCallback(int bufferSize, const char *buffer, void *callbackData) {
      auto req = static_cast<S3RequestWrapper*>(callbackData);
      auto offset = req->req->buffer.size();
      req->req->buffer.resize(offset + bufferSize); // out of memory exception?
      std::copy_n(buffer, bufferSize, req->req->buffer.begin() + offset);
      return S3StatusOK; // can also return S3StatusAbortedByCallback
    }

    constexpr static S3ResponseHandler responseHandler{
      &S3LibWrapper::responsePropertiesCallback,
      &S3LibWrapper::responseCompleteCallback
    };

    constexpr static S3PutObjectHandler putObjectHandler{
      responseHandler,
      &S3LibWrapper::putObjectDataCallback
    };

    constexpr static S3GetObjectHandler getObjectHandler{
      responseHandler,
      &S3LibWrapper::getObjectDataCallback
    };

  private:
    S3Status initStatus_;
    int asyncRequestLimit_{32}; // no more than FD_SETSIZE (1024)
    int asyncAddRequestLimit_{64};
    std::thread loop_;
    std::atomic<bool> running_;
    // all callbackData pointers are to S3RequestWrapper objects
    tbb::concurrent_queue<S3RequestWrapper*> requests_;
};

} // anon namespace

namespace cce::tf {

std::ostream& operator<<(std::ostream& os, const S3Request& req) {
  os << "S3Request(";
  switch (req.type) {
    case S3Request::Type::undef:
      os << "undef"; break;
    case S3Request::Type::get:
      os << "get"; break;
    case S3Request::Type::put:
      os << "put"; break;
  }
  os << ", key=" << req.key << ", timeout=" << req.timeout.count() << "ms, retries=" << req.retries;
  os << ", buffer length=" << req.buffer.size() << ", ";
  switch (req.status) {
    case S3Request::Status::waiting:
      os << "waiting"; break;
    case S3Request::Status::ok:
      os << "ok"; break;
    case S3Request::Status::error:
      os << "error"; break;
  }
  os << ")";
  return os;
}

S3ConnectionRef S3Connection::from_config(const std::string& filename) {
  std::ifstream fin(filename);
  if (not fin.is_open()) {
    std::cerr << "S3Connection config file " << filename << " could not be opened\n";
    return {};
  }
  std::string hostName;
  std::string bucketName;
  std::string accessKeyId;
  std::string secretAccessKey;
  std::string securityToken;
  for (std::string line; std::getline(fin, line); ) {
    if ( line.empty() || line[0] == '#' ) continue;
    auto delim = line.find("=");
    auto key = line.substr(0, delim);
    auto val = line.substr(delim+1, line.length() - 1);
    if ( key == "hostName" ) hostName = val;
    else if ( key == "bucketName" ) bucketName = val;
    else if ( key == "accessKeyId" ) accessKeyId = val;
    else if ( key == "secretAccessKey" ) secretAccessKey = val;
    else if ( key == "securityToken" ) securityToken = val;
    else {
      std::cerr << "unrecognized config file key " << key << " in S3Connection config " << filename << "\n";
    }
  }

  if ( hostName.empty() || bucketName.empty() || accessKeyId.empty() || secretAccessKey.empty() ) {
    std::cerr << "S3Connection config file missing required keys\n";
    return {};
  }

  if ( not S3LibWrapper::instance().running() ) {
    return {};
  }

  S3Status status = S3_validate_bucket_name(bucketName.c_str(), S3UriStyleVirtualHost);
  if ( status != S3StatusOK ) {
    std::cerr << "S3 bucket name invalid: " << bucketName << "\n";
    return {};
  }

  return std::make_shared<const S3Connection>(hostName, bucketName, accessKeyId, secretAccessKey, securityToken);
};

S3Connection::S3Connection(
    std::string_view iHostName,
    std::string_view iBucketName,
    std::string_view iAccessKey,
    std::string_view iSecretKey,
    std::string_view iSecurityToken
    ) :
  hostName_(iHostName),
  bucketName_(iBucketName),
  accessKeyId_(iAccessKey),
  secretAccessKey_(iSecretKey),
  securityToken_(iSecurityToken),
  blockingTime_{0}
{
  if ( hostName_ == "devnull") {
    // magic do-nothing connection
    return;
  }
  ctx_.reset(new S3BucketContext{
    .hostName = hostName_.c_str(),
    .bucketName = bucketName_.c_str(),
    .protocol = S3ProtocolHTTPS,
    .uriStyle = S3UriStylePath,
    .accessKeyId = accessKeyId_.c_str(),
    .secretAccessKey = secretAccessKey_.c_str(),
    .securityToken = securityToken_.empty() ? nullptr : securityToken_.c_str(),
    .authRegion = nullptr
  });
};

void S3Connection::submit(std::shared_ptr<S3Request> req, TaskHolder&& callback) const {
  auto start = std::chrono::high_resolution_clock::now();
  if ( ctx_ ) {
    auto task_handle = callback.group()->defer([cb=std::move(callback)](){});
    // start of S3RequestWrapper lifecycle (ends in S3LibWrapper::responseCompleteCallback)
    auto wrapper = new S3RequestWrapper(std::move(req), ctx_.get(), std::move(task_handle));
    S3LibWrapper::instance().submit(wrapper);
  } else {
    if ( req->type == S3Request::Type::put ) {
      req->status = S3Request::Status::ok;
    } else {
      req->status = S3Request::Status::error;
    }
    callback.doneWaiting();
  }
  auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
  blockingTime_ += time.count();
};

}
