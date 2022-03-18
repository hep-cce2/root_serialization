#include "S3Outputer.h"
#include "OutputerFactory.h"
#include <iostream>

namespace cce::tf {
namespace {
    class Maker : public OutputerMakerBase {
  public:
    Maker(): OutputerMakerBase("S3Outputer") {}
    std::unique_ptr<OutputerBase> create(unsigned int iNLanes, ConfigurationParameters const& params) const final {
      bool verbose = params.get<bool>("verbose",false);
      return std::make_unique<S3Outputer>(iNLanes, verbose);
    }
    };

  Maker s_maker;
}
}
