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
#include "tbb/task_arena.h"
#include "tbb/task_group.h"
#include "tbb/concurrent_queue.h"
#include "S3Common.h"
#include "FunctorTask.h"


namespace {
using namespace cce::tf;

class S3RequestWrapper {
  public:
    S3RequestWrapper(S3Request::Ptr iReq, const S3BucketContext* iCtx, S3Request::Callback iCb, tbb::task_group* iGroup):
      req{std::move(iReq)}, bucketCtx{iCtx}, callback{iCb}, group{iGroup}
    {
      if ( group != nullptr ) {
        arena = std::make_unique<tbb::task_arena>(tbb::task_arena::attach{});
      }
      backoffTimeout = req->timeout.count();
    };

    inline bool isAsync() const { return group != nullptr; };
    void done() {
      if ( group == nullptr ) {
        callback(std::move(req));
      } else {
        // could not figure out how to capture the unique_ptr properly... so release and remake
        auto task = [cb=std::move(callback), ptr=req.release()]() {
          cb(S3Request::Ptr(ptr));
        };
        arena->enqueue([group=group, task=std::move(task)]() { group->run(task); });
      }
    };

    S3Request::Ptr req;
    const S3BucketContext* bucketCtx;
    const S3Request::Callback callback;
    tbb::task_group* group{nullptr};
    std::unique_ptr<tbb::task_arena> arena;
    size_t put_offset{0};
    int retries_executed{0};
    long backoffTimeout;
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
      if ( req->isAsync() ) {
        requests_.push(req);
      } else {
        _submit(req, nullptr);
      }
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
      S3_create_request_context(&ctx);
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

        S3RequestWrapper* req;
        int currentlyActive{activeRequests};
        while (
            (activeRequests < asyncRequestLimit_)
            and activeRequests < (currentlyActive+asyncAddRequestLimit_)
            and requests_.try_pop(req) // test this last!
            ) {
          _submit(req, ctx);
          activeRequests++;
        }
        if ( activeRequests == 0 ) {
          // TODO: would be better to use a semaphore (submit() and ~S3LibWrapper need to notify)
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      }
      // TODO: this may abort requests in flight, do we wait or is it synchronous?
      S3_destroy_request_context(ctx);
    }

    void _submit(S3RequestWrapper* req, S3RequestContext* ctx) const {
      // this function will block if ctx is null
      assert(req->isAsync() xor ctx == nullptr);
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
      if ( S3_status_is_retryable(status) && req->retries_executed < req->req->retries ) {
        if ( status == S3Status::S3StatusErrorRequestTimeout ) {
          // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
          static thread_local std::minstd_rand rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
          std::uniform_int_distribution dist(0l, std::min(S3Request::max_timeout.count(), req->backoffTimeout));
          auto dt = std::chrono::milliseconds(dist(rng));
          if ( req->isAsync() ) {
            // TODO: async sleep by setting a future submit time and checking in loop_body
          } else {
            // TODO: better option?
            std::this_thread::sleep_for(dt);
            req->backoffTimeout *= 2;
          }
        } else {
          std::cerr << "Got status " << S3_get_status_name(status) << " while running request " << *(req->req) << ", retrying\n";
        }
        req->put_offset = 0;
        req->retries_executed++;
        if ( req->isAsync() ) {
          instance().requests_.push(req);
        } else {
          // can libs3 callbacks recurse? probably...
          instance()._submit(req, nullptr);
        }
        return; // no delete!
      }
      switch ( status ) {
        case S3StatusOK:
          req->req->status = S3Request::Status::ok;
          break;
        default:
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
    int asyncRequestLimit_{256};
    int asyncAddRequestLimit_{16}; // TODO: when this is a reasonable number, there's a race
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
    .protocol = S3ProtocolHTTP,
    .uriStyle = S3UriStylePath,
    .accessKeyId = accessKeyId_.c_str(),
    .secretAccessKey = secretAccessKey_.c_str(),
    .securityToken = securityToken_.empty() ? nullptr : securityToken_.c_str(),
    .authRegion = nullptr
  });
};

void S3Connection::get(const std::string& key, tbb::task_group* group, S3Request::Callback&& cb) const {
  auto start = std::chrono::high_resolution_clock::now();
  if ( ctx_ ) {
    auto req = std::make_unique<S3Request>(S3Request::Type::get, key);
    // start of S3RequestWrapper lifecycle (ends in S3LibWrapper::responseCompleteCallback)
    auto wrapper = new S3RequestWrapper(std::move(req), ctx_.get(), std::move(cb), group);
    S3LibWrapper::instance().submit(wrapper);
  } else if ( cb ) {
    auto dummy = std::make_unique<S3Request>(S3Request::Type::get, key);
    dummy->status = S3Request::Status::error;
    cb(std::move(dummy));
  }
  auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
  blockingTime_ += time.count();
};

void S3Connection::put(const std::string& key, std::string&& value, tbb::task_group* group, S3Request::Callback&& cb) const {
  auto start = std::chrono::high_resolution_clock::now();
  if ( ctx_ ) {
    auto req = std::make_unique<S3Request>(S3Request::Type::put, key);
    req->buffer = std::move(value);
    // start of S3RequestWrapper lifecycle (ends in S3LibWrapper::responseCompleteCallback)
    auto wrapper = new S3RequestWrapper(std::move(req), ctx_.get(), std::move(cb), group);
    S3LibWrapper::instance().submit(wrapper);
  } else if ( cb ) {
    auto dummy = std::make_unique<S3Request>(S3Request::Type::put, key);
    dummy->buffer = std::move(value);
    dummy->status = S3Request::Status::ok;
    cb(std::move(dummy));
  }
  auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
  blockingTime_ += time.count();
};

}
