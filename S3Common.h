#if !defined(S3Common_h)
#define S3Common_h

#include <memory>
#include <mutex>

// libs3.h
struct S3BucketContext;

namespace cce::tf {
class S3LibWrapper;
class S3Connection;
typedef std::shared_ptr<S3Connection> S3ConnectionRef;

struct S3Request {
  enum class Type {undef, get, put}; 
  enum class Status {ok, error}; 
  typedef std::function<void(S3Request*)> Callback;

  const Type type{Type::undef};
  const S3BucketContext* bucketCtx{nullptr};
  const std::string key;
  const Callback callback;
  std::string buffer;
  int timeout{1000}; // milliseconds
  int retriesRemaining{3};
  Status status;
  // "private"
  S3LibWrapper *const owner{nullptr};
  size_t put_offset{0};
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
