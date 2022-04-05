#if !defined(PHDFBatchEventsOutputer_h)
#define PHDFBatchEventsOutputer_h


#include <vector>
#include <string>
#include <cstdint>
#include <fstream>


#include "OutputerBase.h"
#include "EventIdentifier.h"
#include "SerializeStrategy.h"
#include "DataProductRetriever.h"
#include "pds_writer.h"

#include "SerialTaskQueue.h"

#include "HDFCxx.h"


namespace cce::tf {
  class PHDFBatchEventsOutputer : public OutputerBase {
    public:
    enum class CompressionChoice {
      kNone,
        kEvents,
        kBatch,
        kBoth
    };

    PHDFBatchEventsOutputer(std::string const& iFileName, unsigned int iNLanes, int iChunkSize, pds::Compression iCompression, int iCompressionLevel, CompressionChoice iChoice, pds::Serialization iSerialization, uint32_t iBatchSize);
    PHDFBatchEventsOutputer(PHDFBatchEventsOutputer&&) = default;
    PHDFBatchEventsOutputer(PHDFBatchEventsOutputer const&) = default;

  void setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) final;

  void productReadyAsync(unsigned int iLaneIndex, DataProductRetriever const& iDataProduct, TaskHolder iCallback) const final;
  bool usesProductReadyAsync() const final {return true;}

  void outputAsync(unsigned int iLaneIndex, EventIdentifier const& iEventID, TaskHolder iCallback) const final;
  
  void printSummary() const final;

 private:

  void finishBatchAsync(unsigned int iBatchIndex, TaskHolder iCallback);

  void output(std::vector<EventIdentifier> iEventID, std::vector<char> iBuffer, std::vector<uint32_t> iOffset);
  void writeFileHeader(SerializeStrategy const& iSerializers);
  std::pair<std::vector<uint32_t>,std::vector<char>> writeDataProductsToOutputBuffer(SerializeStrategy const& iSerializers) const;

private:
  hdf5::File file_;
  hdf5::Group group_;
  mutable SerialTaskQueue queue_;
  int chunkSize_;
  mutable std::vector<SerializeStrategy> serializers_;

  //This is used as a circular buffer of length nLanes but only entries being used exist
  using EventInfo = std::tuple<EventIdentifier, std::vector<uint32_t>, std::vector<char>>;
  mutable std::vector<std::atomic<std::vector<EventInfo>*>> eventBatches_;
  mutable std::vector<std::atomic<uint32_t>> waitingEventsInBatch_;

  mutable std::atomic<uint64_t> presentEventEntry_;

  uint32_t batchSize_;
  bool firstEvent_ = true;
  pds::Compression compression_;
  int compressionLevel_;
  CompressionChoice compressionChoice_;
  pds::Serialization serialization_;
  mutable std::chrono::microseconds serialTime_;
  mutable std::atomic<std::chrono::microseconds::rep> parallelTime_;
  };    
}
#endif
