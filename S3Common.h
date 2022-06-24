#if !defined(S3Common_h)
#define S3Common_h

#include <chrono>
#include <memory>
#include <mutex>
#include <ostream>

// libs3.h
struct S3BucketContext;

namespace cce::tf {
class S3LibWrapper;
class S3Connection;
typedef std::shared_ptr<S3Connection> S3ConnectionRef;

class S3Request {
  public:
    enum class Type {undef, get, put};
    enum class Status {waiting, ok, error};
    typedef std::function<void(S3Request*)> Callback;
    static constexpr std::chrono::milliseconds max_timeout{60000};

    const Type type;
    const S3BucketContext* bucketCtx;
    const std::string key;
    const Callback callback;
    const std::chrono::milliseconds timeout{1000};
    const int retries{5};
    std::string buffer;
    Status status;

  private:
    S3Request() = delete;
    // constructor for devnull connection
    S3Request(Type iType, std::string iKey, Status stat):
      type{iType}, key{iKey}, status{stat} {};
    // get constructor
    S3Request(Type iType, const S3BucketContext* iCtx, std::string iKey, Callback iCb, S3LibWrapper* iOwner):
      type{iType}, bucketCtx{iCtx}, key{iKey}, callback{iCb}, _owner{iOwner}
    {
      _timeout = timeout.count();
    };
    // put constructor
    S3Request(Type iType, const S3BucketContext* iCtx, std::string iKey, Callback iCb, S3LibWrapper* iOwner, std::string&& buf):
      type{iType}, bucketCtx{iCtx}, key{iKey}, callback{iCb}, _owner{iOwner}, buffer{buf}
    {
      _timeout = timeout.count();
    };

    S3LibWrapper *const _owner{nullptr};
    size_t _put_offset{0};
    int _retries_executed{0};
    long _timeout;

  friend class S3LibWrapper;
  friend class S3Connection;
  friend std::ostream& operator<<(std::ostream& os, const S3Request& req);
};


class S3Connection {
  public:
    static S3ConnectionRef from_config(std::string filename);

    S3Connection(
        std::string_view iHostName,
        std::string_view iBucketName,
        std::string_view iAccessKey,
        std::string_view iSecretKey,
        std::string_view iSecurityToken
        );

    void get(const std::string key, S3Request::Callback&& cb);
    void put(const std::string key, std::string&& value, S3Request::Callback&& cb);

  private:
    const std::string hostName_;
    const std::string bucketName_;
    const std::string accessKeyId_;
    const std::string secretAccessKey_;
    const std::string securityToken_;
    // holds pointers to c_str() of the above
    std::unique_ptr<const S3BucketContext> ctx_;
};

}
#endif
