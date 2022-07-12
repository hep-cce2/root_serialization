#if !defined(S3Common_h)
#define S3Common_h
#include <chrono>
#include <memory>
#include <mutex>
#include <ostream>

// libs3.h
struct S3BucketContext;

namespace cce::tf {
class S3Connection;
typedef std::shared_ptr<const S3Connection> S3ConnectionRef;

class S3Request {
  public:
    enum class Type {undef, get, put};
    enum class Status {waiting, ok, error};
    typedef std::unique_ptr<S3Request> Ptr;
    typedef std::function<void(S3Request::Ptr)> Callback;
    static constexpr std::chrono::milliseconds max_timeout{60000};

    S3Request() = delete;
    S3Request(Type iType, const std::string& iKey, std::chrono::milliseconds iTimeout=std::chrono::milliseconds(1000), int iRetries=5):
      type{iType}, key{iKey}, timeout{iTimeout}, retries{iRetries} {};

    const Type type;
    const std::string key;
    const std::chrono::milliseconds timeout;
    const int retries;
    std::string buffer;
    Status status{Status::waiting};

    friend std::ostream& operator<<(std::ostream& os, const S3Request& req);
};

class S3Connection {
  public:
    static S3ConnectionRef from_config(const std::string& filename);

    S3Connection(
        std::string_view iHostName,
        std::string_view iBucketName,
        std::string_view iAccessKey,
        std::string_view iSecretKey,
        std::string_view iSecurityToken
        );

    // if group == nullptr, these functions execute synchronously
    // else, the request will execute async and schedule the callback to run in the group when done
    void get(const std::string& key, tbb::task_group* group, S3Request::Callback&& cb) const;
    void put(const std::string& key, std::string&& value, tbb::task_group* group, S3Request::Callback&& cb) const;
    std::chrono::microseconds blockingTime() const { return std::chrono::microseconds(blockingTime_.load()); }

  private:
    const std::string hostName_;
    const std::string bucketName_;
    const std::string accessKeyId_;
    const std::string secretAccessKey_;
    const std::string securityToken_;
    // holds pointers to c_str() of the above
    std::unique_ptr<const S3BucketContext> ctx_;

    mutable std::atomic<std::chrono::microseconds::rep> blockingTime_;
};

}
#endif
