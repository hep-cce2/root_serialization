#include "outputerFactoryGenerator.h"
#include "OutputerFactory.h"
#include "configKeyValuePairs.h"
#include <iostream>

std::function<std::unique_ptr<cce::tf::OutputerBase>(unsigned int)>
cce::tf::outputerFactoryGenerator(std::string_view iType, std::string_view iOptions) {
  std::function<std::unique_ptr<OutputerBase>(unsigned int)> outFactory;

  auto keyValues = cce::tf::configKeyValuePairs(iOptions);
  outFactory = [type=std::string(iType), params=ConfigurationParameters(keyValues)](unsigned int iNLanes) {
    auto maker = OutputerFactory::get()->create(type, iNLanes, params);
    if(not maker) {
      return maker;
    }
    
    //make sure all parameters given were used
    auto unusedOptions = params.unusedKeys();
    if(not unusedOptions.empty()) {
      std::cout <<"Unused options in "<<type<<"\n";
      for(auto const& key: unusedOptions) {
        std::cout <<"  '"<<key<<"'"<<std::endl;
      }
      return decltype(maker)();
    }

    return maker;
  };
  return outFactory;
}
