#if !defined(HDFOutputer_h)
#define HDFOutputer_h


#include <vector>
#include <string>
#include <cstdint>
#include <fstream>


#include "OutputerBase.h"
#include "EventIdentifier.h"
#include "SerializerWrapper.h"
#include "DataProductRetriever.h"

#include "SerialTaskQueue.h"

#include "HDFCxx.h"

using product_t = std::vector<char>;

namespace cce::tf {
  class HDFOutputer : public OutputerBase {
    public:
    HDFOutputer(std::string const& iFileName, unsigned int iNLanes, int iBatchSize, int iChunkSize);
    HDFOutputer(HDFOutputer&&) = default;
    HDFOutputer(HDFOutputer const&) = default;
    ~HDFOutputer();

  void setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) final;

  void productReadyAsync(unsigned int iLaneIndex, DataProductRetriever const& iDataProduct, TaskHolder iCallback) const final;
  bool usesProductReadyAsync() const final {return true;}

  void outputAsync(unsigned int iLaneIndex, EventIdentifier const& iEventID, TaskHolder iCallback) const final;
  
  void printSummary() const final;

 private:

  void output(EventIdentifier const& iEventID, std::vector<SerializerWrapper> const& iSerializers);
  void writeFileHeader(EventIdentifier const& iEventID, std::vector<SerializerWrapper> const& iSerializers);

  std::vector<std::vector<char>> writeDataProductsToOutputBuffer(std::vector<SerializerWrapper> const& iSerializers) const;
  std::pair<product_t, std::vector<size_t>> get_prods_and_sizes(std::vector<product_t> & input,int prod_index,int stride);
  std::pair<product_t, std::vector<size_t>> get_prods_and_sizes_collective(std::vector<product_t> & input,int prod_index,int stride);

 private:
 void writeBatch();
 void writeBatch_Coll();

  hdf5::File file_;
  mutable SerialTaskQueue queue_;
  int chunkSize_;
  int maxBatchSize_;
  std::vector<std::pair<std::string, uint32_t>> dataProductIndices_;
  mutable std::vector<std::vector<SerializerWrapper>> serializers_;
  bool firstTime_ = true;
  
  std::vector<product_t> products_; 
  std::vector<size_t> offsets_ = decltype(offsets_)(1000, 0);
  mutable int batch_ = 0;
  std::vector<int> events_;
  
  mutable std::chrono::microseconds serialTime_;
  mutable std::atomic<std::chrono::microseconds::rep> parallelTime_;
  };    
}
#endif
