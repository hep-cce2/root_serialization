#include "SharedPDSSource.h"
#include "SourceFactory.h"
#include "Deserializer.h"
#include "UnrolledDeserializer.h"

#include "TClass.h"

using namespace cce::tf;

SharedPDSSource::SharedPDSSource(unsigned int iNLanes, unsigned long long iNEvents, std::string const& iName) :
                 SharedSourceBase(iNEvents),
                 file_{iName, std::ios_base::binary},
  readTime_{std::chrono::microseconds::zero()}
{
  pds::Serialization serialization;
  auto productInfo = readFileHeader(file_, compression_, serialization);

  laneInfos_.reserve(iNLanes);
  for(unsigned int i = 0; i< iNLanes; ++i) {
    DeserializeStrategy strategy;
    switch(serialization) {
    case pds::Serialization::kRoot: { 
      strategy = DeserializeStrategy::make<DeserializeProxy<Deserializer>>(); break;
    }
    case pds::Serialization::kRootUnrolled: {
      strategy = DeserializeStrategy::make<DeserializeProxy<UnrolledDeserializer>>(); break;
    }
    }
    laneInfos_.emplace_back(productInfo, std::move(strategy));
  }
}

SharedPDSSource::LaneInfo::LaneInfo(std::vector<pds::ProductInfo> const& productInfo, DeserializeStrategy deserialize):
  deserializers_{std::move(deserialize)},
  decompressTime_{std::chrono::microseconds::zero()},
  deserializeTime_{std::chrono::microseconds::zero()}
{
  dataProducts_.reserve(productInfo.size());
  dataBuffers_.resize(productInfo.size(), nullptr);
  deserializers_.reserve(productInfo.size());
  size_t index =0;
  for(auto const& pi : productInfo) {
    
    TClass* cls = TClass::GetClass(pi.className().c_str());
    dataBuffers_[index] = cls->New();
    assert(cls);
    dataProducts_.emplace_back(index,
			       &dataBuffers_[index],
                               pi.name(),
                               cls,
			       &delayedRetriever_);
    deserializers_.emplace_back(cls);
    ++index;
  }
}

SharedPDSSource::LaneInfo::~LaneInfo() {
  auto it = dataProducts_.begin();
  for( void * b: dataBuffers_) {
    it->classType()->Destructor(b);
    ++it;
  }
}

size_t SharedPDSSource::numberOfDataProducts() const {
  return laneInfos_[0].dataProducts_.size();
}

std::vector<DataProductRetriever>& SharedPDSSource::dataProducts(unsigned int iLane, long iEventIndex) {
  return laneInfos_[iLane].dataProducts_;
}

EventIdentifier SharedPDSSource::eventIdentifier(unsigned int iLane, long iEventIndex) {
  return laneInfos_[iLane].eventID_;
}

void SharedPDSSource::readEventAsync(unsigned int iLane, long iEventIndex,  OptionalTaskHolder iTask) {
  queue_.push(*iTask.group(), [iLane, optTask = std::move(iTask), this]() mutable {

      auto start = std::chrono::high_resolution_clock::now();
      std::vector<uint32_t> buffer;
      
      if(pds::readCompressedEventBuffer(file_, this->laneInfos_[iLane].eventID_, buffer)) {
        //last entry in buffer is just a crosscheck on its size
        buffer.pop_back();
        auto group = optTask.group();
        group->run([this, buffer=std::move(buffer), task = optTask.releaseToTaskHolder(), iLane]() {
            auto& laneInfo = this->laneInfos_[iLane];

            auto start = std::chrono::high_resolution_clock::now();
            std::vector<uint32_t> uBuffer = pds::uncompressEventBuffer(this->compression_, buffer);
            laneInfo.decompressTime_ += 
              std::chrono::duration_cast<decltype(laneInfo.decompressTime_)>(std::chrono::high_resolution_clock::now() - start);
            
            start = std::chrono::high_resolution_clock::now();
            pds::deserializeDataProducts(uBuffer.begin(), uBuffer.end(), laneInfo.dataProducts_, laneInfo.deserializers_);
            laneInfo.deserializeTime_ += 
              std::chrono::duration_cast<decltype(laneInfo.deserializeTime_)>(std::chrono::high_resolution_clock::now() - start);
          });
      }
      readTime_ +=std::chrono::duration_cast<decltype(readTime_)>(std::chrono::high_resolution_clock::now() - start);
    });
}

void SharedPDSSource::printSummary() const {
  std::cout <<"\nSource:\n"
    "   read time: "<<readTime().count()<<"us\n"
    "   decompress time: "<<decompressTime().count()<<"us\n"
    "   deserialize time: "<<deserializeTime().count()<<"us\n"<<std::endl;
};

std::chrono::microseconds SharedPDSSource::readTime() const {
  return readTime_;
}

std::chrono::microseconds SharedPDSSource::decompressTime() const {
  auto time = std::chrono::microseconds::zero();
  for(auto const& l : laneInfos_) {
    time += l.decompressTime_;
  }
  return time;
}

std::chrono::microseconds SharedPDSSource::deserializeTime() const {
  auto time = std::chrono::microseconds::zero();
  for(auto const& l : laneInfos_) {
    time += l.deserializeTime_;
  }
  return time;
}


namespace {
    class Maker : public SourceMakerBase {
  public:
    Maker(): SourceMakerBase("SharedPDSSource") {}
      std::unique_ptr<SharedSourceBase> create(unsigned int iNLanes, unsigned long long iNEvents, ConfigurationParameters const& params) const final {
        auto fileName = params.get<std::string>("fileName");
        if(not fileName) {
          std::cout <<"no file name given\n";
          return {};
        }
        return std::make_unique<SharedPDSSource>(iNLanes, iNEvents, *fileName);
    }
    };

  Maker s_maker;
}
