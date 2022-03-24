#include "ConfigurationParameters.h"

namespace cce::tf {
  template<>
    std::string ConfigurationParameters::convert<std::string>(std::string const& iValue) {
    return iValue;
  }
  
  template<>
    int ConfigurationParameters::convert<int>(std::string const& iValue) {
    return std::stoi(iValue);
  }

  template<>
    unsigned int ConfigurationParameters::convert<unsigned int>(std::string const& iValue) {
    return std::stoul(iValue);
  }

  template<>
    float ConfigurationParameters::convert<float>(std::string const& iValue) {
    return std::stof(iValue);
  }

  template<>
    unsigned long ConfigurationParameters::convert<unsigned long>(std::string const& iValue) {
    return std::stoul(iValue);
  }

  template<>
  bool ConfigurationParameters::convert<bool>(std::string const& iValue) {
    return (iValue.empty()) or (
                                     (iValue[0] == 't') or (iValue[0] == 'T') or (iValue[0] == 'y') or (iValue[0] == 'Y')
                                     );
  }
}
