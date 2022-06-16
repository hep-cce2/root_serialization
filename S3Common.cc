#include <atomic>
#include <cassert>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <thread>
#include <variant>

#include "libs3.h"
#include "tbb/concurrent_queue.h"
#include "S3Common.h"


namespace cce::tf {

class S3LibWrapper {
  public:
    S3LibWrapper(bool async=false) : async_(async), running_(false) {
      initStatus_ = S3_initialize("s3", S3_INIT_ALL, "");
      if ( initStatus_ != S3StatusOK ) {
        std::cerr << "Failed to initialize libs3, error: " << S3_get_status_name(initStatus_) << "\n";
        return;
      }
      running_ = true;
      if ( async_ ) {
        throw std::runtime_error("Async not supported yet");
        loop_ = std::thread(&S3LibWrapper::loop_body, this);
      }
    }

    ~S3LibWrapper() {
      running_ = false;
      if ( loop_.joinable() ) loop_.join();
      S3_deinitialize();
    }

    bool isAsync() { return async_; }
    bool running() { return running_; }

    void get(const S3BucketContext* bucketCtx, const std::string key, S3Request::Callback&& cb) {
      // start of S3Request lifecycle (s3lib will always call responseCompleteCallback)
      auto req = new S3Request{
        .type = S3Request::Type::get,
        .bucketCtx = bucketCtx,
        .key = key,
        .callback = std::move(cb),
        .owner = this,
      };
      if ( async_ ) {
        requests_.push(req);
      } else {
        submit(req);
      }
    }

    void put(const S3BucketContext* bucketCtx, const std::string key, std::string&& value, S3Request::Callback&& cb) {
      // start of S3Request lifecycle (s3lib will always call responseCompleteCallback)
      auto req = new S3Request{
        .type = S3Request::Type::put,
        .bucketCtx = bucketCtx,
        .key = key,
        .callback = std::move(cb),
        .buffer = std::move(value),
        .owner = this,
      };
      if ( async_ ) {
        requests_.push(req);
      } else {
        submit(req);
      }
    }

  private:
    void loop_body() {
      S3_create_request_context(&requestContext_);
      while(running_) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1s);

        // S3Status S3_get_request_context_fdsets(S3RequestContext *requestContext, fd_set *readFdSet, fd_set *writeFdSet, fd_set *exceptFdSet, int *maxFd);
        // int64_t S3_get_request_context_timeout(S3RequestContext *requestContext); // milliseconds
        // select()
        // S3Status S3_runonce_request_context(S3RequestContext *requestContext, int *requestsRemainingReturn);

        // S3Request* req;
        // concurrency limit?
        // while ( requests_.try_pop(req) ) submit(req);
      }
      S3_destroy_request_context(requestContext_);
    }

    void submit(S3Request* req) {
      // this function will not block if requestContext_ is not null
      // which should only be the case if async_ is true
      switch ( req->type ) {
        case S3Request::Type::undef:
          assert(false); // logic error
          break;
        case S3Request::Type::get:
          S3_get_object(
              req->bucketCtx,
              req->key.c_str(),
              nullptr, // S3GetConditions
              0, // startByte
              0, // byteCount
              requestContext_,
              req->timeout,
              &S3LibWrapper::getObjectHandler,
              static_cast<void*>(req));
          break;
        case S3Request::Type::put:
          S3_put_object(
              req->bucketCtx,
              req->key.c_str(),
              req->buffer.size(),
              nullptr, // S3PutProperties (TODO probably want .md5)
              requestContext_,
              req->timeout,
              &S3LibWrapper::putObjectHandler,
              static_cast<void*>(req));
          break;
      }
    }

    static S3Status responsePropertiesCallback(const S3ResponseProperties *properties, void *callbackData) {
      auto req = static_cast<S3Request*>(callbackData);
      if ( req->type == S3Request::Type::get ) {
        if ( properties->contentLength > 0 ) {
          req->buffer.reserve(properties->contentLength);
        }
        // else what?
        // TODO: save headers?
      }
      return S3StatusOK;
      // perhaps S3StatusAbortedByCallback
    }

    static void responseCompleteCallback(S3Status status, const S3ErrorDetails *error, void *callbackData) {
      auto req = static_cast<S3Request*>(callbackData);
      if ( S3_status_is_retryable(status) && req->retriesRemaining > 0 ) {
        req->retriesRemaining--;
        // TODO: back-off algo?
        // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        if ( req->owner->async_ ) {
          req->owner->requests_.push(req);
        } else {
          // can libs3 callbacks recurse? probably...
          req->owner->submit(req);
        }
        return; // no delete!
      }
      switch ( status ) {
        case S3StatusOK:
          req->status = S3Request::Status::ok;
          break;
        default:
          req->status = S3Request::Status::error;
      }
      if ( req->callback ) req->callback(req);
      // end of S3Request lifecycle (s3lib will always call responseCompleteCallback)
      delete req;
    }

    static int putObjectDataCallback(int bufferSize, char *buffer, void *callbackData) {
      auto req = static_cast<S3Request*>(callbackData);
      size_t toWrite = std::min(bufferSize, (int) (req->buffer.size() - req->put_offset));
      if ( toWrite > 0 ) {
        std::copy_n(req->buffer.begin() + req->put_offset, toWrite, buffer);
        req->put_offset += toWrite;
      }
      // return > 0 = bytes written, 0 = done, -1 = S3StatusAbortedByCallback
      return toWrite;
    }

    static S3Status getObjectDataCallback(int bufferSize, const char *buffer, void *callbackData) {
      auto req = static_cast<S3Request*>(callbackData);
      auto offset = req->buffer.size();
      req->buffer.resize(offset + bufferSize); // out of memory exception?
      std::copy_n(buffer, bufferSize, req->buffer.begin() + offset);
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
    bool async_;
    std::thread loop_;
    std::atomic<bool> running_;
    S3RequestContext* requestContext_{nullptr};
    // all callbackData pointers are to S3Request objects
    tbb::concurrent_queue<S3Request*> requests_;
};

// libs3 asks us to initialize and de-initialize once per process
// optional TODO: make it a singleton and only initialize when needed
S3LibWrapper s3lib;


S3ConnectionRef S3Connection::from_config(std::string filename) {
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

  if ( not s3lib.running() ) {
    return {};
  }

  S3Status status = S3_validate_bucket_name(bucketName.c_str(), S3UriStyleVirtualHost);
  if ( status != S3StatusOK ) {
    std::cerr << "S3 bucket name invalid: " << bucketName << "\n";
    return {};
  }

  return std::make_shared<S3Connection>(hostName, bucketName, accessKeyId, secretAccessKey, securityToken);
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
  securityToken_(iSecurityToken)
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

void S3Connection::get(const std::string key, S3Request::Callback&& cb) {
  if ( ctx_ ) {
    s3lib.get(ctx_.get(), key, std::move(cb));
  } else if ( cb ) {
    S3Request dummy{
      .type = S3Request::Type::get,
      .key = key,
      .status = S3Request::Status::error
    };
    cb(&dummy);
  }
};

void S3Connection::put(const std::string key, std::string&& value, S3Request::Callback&& cb) {
  if ( ctx_ ) {
    s3lib.put(ctx_.get(), key, std::move(value), std::move(cb));
  } else if ( cb ) {
    S3Request dummy{
      .type = S3Request::Type::put,
      .key = key,
      .status = S3Request::Status::ok
    };
    cb(&dummy);
  }
};

}
