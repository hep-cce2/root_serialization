#include "S3Outputer.h"
#include "OutputerFactory.h"
#include <iostream>

namespace cce::tf {
namespace {
    class Maker : public OutputerMakerBase {
  public:
    Maker(): OutputerMakerBase("S3Outputer") {}
    std::unique_ptr<OutputerBase> create(unsigned int iNLanes, ConfigurationParameters const& params) const final {
      auto verbose = params.get<int>("verbose", 0);
      auto productFlush = params.get<size_t>("productFlush", 1024*512);
      auto eventFlush = params.get<size_t>("eventFlush", 24);
      auto connfile = params.get<std::string>("conn");
      if(not connfile) {
        std::cerr <<"no connection configuration file name given for S3Outputer\n";
        return {};
      }
      auto conn = S3Connection::from_config(connfile.value()); 
      if(not conn) {
        return {};
      }

      std::vector<char> tmp;
      conn->get("testkey", [&tmp](S3Request* req) mutable {
          if ( req->status == S3Request::Status::ok ) {
            std::swap(req->buffer, tmp);
          }
          else { std::cout << "no key" << std::endl; }
        });
      std::string_view s(tmp.data(), tmp.size());
      std::cout << tmp.size() << ": " << s << std::endl;

      conn->put("testkey2", std::move(tmp), [](S3Request* req) {
          if ( req->status == S3Request::Status::ok ) {
            std::cout << "wrote something!" << std::endl;
          }
        });

      return std::make_unique<S3Outputer>(iNLanes, verbose, productFlush, eventFlush, conn);
    }
    };

  Maker s_maker;
}
}
