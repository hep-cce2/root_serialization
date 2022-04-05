#include "PHDFBatchEventsOutputer.h"
#include "OutputerFactory.h"
#include "ConfigurationParameters.h"
#include "UnrolledSerializerWrapper.h"
#include "SerializerWrapper.h"
#include "summarize_serializers.h"
#include "FunctorTask.h"
#include <memory>
#include <iostream>
#include <cstring>
#include <cmath>
#include <set>

using namespace cce::tf;
using namespace cce::tf::pds;

namespace {
  constexpr const char* const PRODUCTS_DSNAME="Products";
  constexpr const char* const EVENTS_DSNAME="EventIDs";
  constexpr const char* const OFFSETS_DSNAME="Offsets";
  constexpr const char* const GNAME="Lumi";
  constexpr const char* const RUN_ANAME="run";
  constexpr const char* const LUMISEC_ANAME="lumisec";
  constexpr const char* const COMPRESSION_ANAME="Compression";
  constexpr const char* const COMPRESSION_LEVEL_ANAME="CompressionLevel";
  constexpr const char* const COMPRESSION_CHOICE_ANAME="CompressionChoice";
  template <typename T> 
  void 
  write_ds(hid_t gid, 
           std::string const& dsname, 
           std::vector<T> const& data) {
    constexpr hsize_t ndims = 1;
    auto dset = hdf5::Dataset::open(gid, dsname.c_str()); 
    auto old_fspace = hdf5::Dataspace::get_space(dset);
    hsize_t max_dims[ndims]; //= {H5S_UNLIMITED};
    hsize_t old_dims[ndims]; //our datasets are 1D
    H5Sget_simple_extent_dims(old_fspace, old_dims, max_dims);
    //now old_dims[0] has existing length
    //we need to extend by the size of data
    hsize_t new_dims[ndims];
    new_dims[0] = old_dims[0] + data.size();
    hsize_t slab_size[ndims];
    slab_size[0] = data.size();
    dset.set_extent(new_dims);
    auto new_fspace = hdf5::Dataspace::get_space (dset);
    new_fspace.select_hyperslab(old_dims, slab_size);
    auto mem_space = hdf5::Dataspace::create_simple(ndims, slab_size, max_dims);
    dset.write<T>(mem_space, new_fspace, data); //H5Dwrite
  }
}

PHDFBatchEventsOutputer::PHDFBatchEventsOutputer(std::string const& iFileName, unsigned int iNLanes, int iChunkSize, pds::Compression iCompression, int iCompressionLevel, CompressionChoice iChoice, pds::Serialization iSerialization, uint32_t iBatchSize) : 
  file_(hdf5::File::parallel_create(iFileName.c_str())),
  group_(hdf5::Group::create(file_, GNAME)),
  chunkSize_{iChunkSize},
  serializers_{std::size_t(iNLanes)},
  eventBatches_{iNLanes},
  waitingEventsInBatch_(iNLanes),
  presentEventEntry_(0),
  batchSize_(iBatchSize),
  compression_{iCompression},
  compressionLevel_{iCompressionLevel},
  compressionChoice_{iChoice},
  serialization_{iSerialization},
  serialTime_{std::chrono::microseconds::zero()},
  parallelTime_{0}
  {
    for(auto& v: waitingEventsInBatch_) {
      v.store(0);
    }
    for(auto& v:eventBatches_) {
      v.store(nullptr);
    }
    
  }


void PHDFBatchEventsOutputer::setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) {
  auto& s = serializers_[iLaneIndex];
  switch(serialization_) {
  case pds::Serialization::kRoot:
    {   s = SerializeStrategy::make<SerializeProxy<SerializerWrapper>>(); break; }
  case pds::Serialization::kRootUnrolled:
    {   s = SerializeStrategy::make<SerializeProxy<UnrolledSerializerWrapper>>(); break; }
  }
  s.reserve(iDPs.size());
  for(auto const& dp: iDPs) {
    s.emplace_back(dp.name(), dp.classType());
  }
  if(iLaneIndex == 0) {
    writeFileHeader(s); 
  }
}

void PHDFBatchEventsOutputer::productReadyAsync(unsigned int iLaneIndex, DataProductRetriever const& iDataProduct, TaskHolder iCallback) const {
  auto& laneSerializers = serializers_[iLaneIndex];
  auto group = iCallback.group();
  laneSerializers[iDataProduct.index()].doWorkAsync(*group, iDataProduct.address(), std::move(iCallback));
}

void PHDFBatchEventsOutputer::outputAsync(unsigned int iLaneIndex, EventIdentifier const& iEventID, TaskHolder iCallback) const {
  auto start = std::chrono::high_resolution_clock::now();
  auto [offsets, buffer] = writeDataProductsToOutputBuffer(serializers_[iLaneIndex]);

  auto eventIndex = presentEventEntry_++;
  auto batchIndex = (eventIndex/batchSize_) % eventBatches_.size();


  auto& batchContainerPtr = eventBatches_[batchIndex];
  auto batchContainer = batchContainerPtr.load();
  if( nullptr == batchContainer ) {
    auto newContainer = std::make_unique<std::vector<EventInfo>>(batchSize_);
    if(batchContainerPtr.compare_exchange_strong(batchContainer, newContainer.get())) {
      newContainer.release();
      batchContainer = batchContainerPtr.load();
    }
  }

  auto indexInBatch = eventIndex % (batchSize_);
  //std::cout <<"indexInBatch "<<indexInBatch<<std::endl;
  (*batchContainer)[indexInBatch] = std::make_tuple(iEventID, std::move(offsets), std::move(buffer));

  assert(waitingEventsInBatch_[batchIndex].load() < batchSize_);

  auto waitingEvents = ++waitingEventsInBatch_[batchIndex];

  if(waitingEvents == batchSize_ ) {
    const_cast<PHDFBatchEventsOutputer*>(this)->finishBatchAsync(batchIndex, std::move(iCallback));
  }
  auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
  parallelTime_ += time.count();
}

void PHDFBatchEventsOutputer::printSummary() const  {
  //make sure last batches are out

  auto start = std::chrono::high_resolution_clock::now();
  {
    tbb::task_group group;
    
    {
      TaskHolder th(group, make_functor_task([](){}));
      for( int index=0; index < waitingEventsInBatch_.size();++index) {
        if(0 != waitingEventsInBatch_[index].load()) {
          const_cast<PHDFBatchEventsOutputer*>(this)->finishBatchAsync(index, th);
        }
      }
    }
    
    group.wait();
  }
  auto writeTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);

  std::cout <<"PHDFBatchEventsOutputer\n  total serial time at end event: "<<serialTime_.count()<<"us\n"
    "  total parallel time at end event: "<<parallelTime_.load()<<"us\n";
  std::cout << "  end of job file write time: "<<writeTime.count()<<"us\n";

  summarize_serializers(serializers_);
}

void PHDFBatchEventsOutputer::finishBatchAsync(unsigned int iBatchIndex, TaskHolder iCallback) {

  std::unique_ptr<std::vector<EventInfo>> batch(eventBatches_[iBatchIndex].exchange(nullptr));
  auto eventsInBatch = waitingEventsInBatch_[iBatchIndex].load();
  waitingEventsInBatch_[iBatchIndex] = 0;

  std::vector<EventIdentifier> batchEventIDs;
  batchEventIDs.reserve(batch->size());

  std::vector<uint32_t> batchOffsets;
  batchOffsets.reserve(batch->size() * (serializers_.size()+2));
  //one extra size to but the final blob size. This is either the
  // compressed size or the uncompressed size depending on the compression choice

  std::vector<char> batchBlob;

  int index = 0;
  for(auto& [id, offsets, blob]: *batch) {
    if(index++ == eventsInBatch) {
      //batch was smaller than usual. Can happen at end of job
      break;
    }
    batchEventIDs.push_back(id);

    std::copy(offsets.begin(), offsets.end(), std::back_inserter(batchOffsets));

    //record the size of the blob as the final offset. Needed to decompress
    // the event during reading
    batchOffsets.push_back(blob.size());
    std::copy(blob.begin(), blob.end(), std::back_inserter(batchBlob));

    //release memory
    blob = {};
  }

  std::vector<char> bufferToWrite;
  if(compressionChoice_ == CompressionChoice::kBatch or compressionChoice_ == CompressionChoice::kBoth) {
    bufferToWrite  = pds::compressBuffer(0,0, compression_, compressionLevel_, batchBlob);
    batchBlob = std::vector<char>();
  } else {
    bufferToWrite = std::move(batchBlob);
  }

  
  queue_.push(*iCallback.group(), [this, eventIDs=std::move(batchEventIDs), offsets = std::move(batchOffsets), buffer = std::move(bufferToWrite),  callback=std::move(iCallback)]() mutable {
      auto start = std::chrono::high_resolution_clock::now();
      const_cast<PHDFBatchEventsOutputer*>(this)->output(std::move(eventIDs), std::move(buffer), std::move(offsets));
        serialTime_ += std::chrono::duration_cast<decltype(serialTime_)>(std::chrono::high_resolution_clock::now() - start);
      callback.doneWaiting();
    });
  
}

void 
PHDFBatchEventsOutputer::output(std::vector<EventIdentifier> iEventIDs, 
                         std::vector<char> iBuffer,
                         std::vector<uint32_t> iOffsets ) {
  if (firstEvent_) {
    assert(not iEventIDs.empty());
    firstEvent_ = false;
    auto r = hdf5::Attribute::open(group_, RUN_ANAME);
    r.write(iEventIDs[0].run);  
    auto sr = hdf5::Attribute::open(group_, LUMISEC_ANAME);
    sr.write(iEventIDs[0].lumi); 
    auto comp = hdf5::Attribute::open(group_, COMPRESSION_ANAME);
    comp.write(std::string(name(compression_)));
    auto level = hdf5::Attribute::open(group_, COMPRESSION_LEVEL_ANAME);
    level.write(compressionLevel_); 
    auto choice = hdf5::Attribute::open(group_, COMPRESSION_CHOICE_ANAME);
    choice.write(static_cast<int>(compressionChoice_)); 
  }
  std::vector<unsigned long long> ids;
  ids.reserve(iEventIDs.size());
  std::transform(iEventIDs.begin(), iEventIDs.end(), std::back_inserter(ids), [](auto const&id) {return id.event;});
  //std::cout <<" ids "<<ids.size()<<std::endl;
  write_ds<unsigned long long>(group_, EVENTS_DSNAME, ids);
  //std::cout <<"wrote ids"<<std::endl;
  write_ds<char>(group_, PRODUCTS_DSNAME, iBuffer);
  write_ds<uint32_t>(group_, OFFSETS_DSNAME, iOffsets); 
}

void 
PHDFBatchEventsOutputer::writeFileHeader(SerializeStrategy const& iSerializers) {
  constexpr hsize_t ndims = 1;
  constexpr hsize_t dims[ndims] = {0};
  hsize_t chunk_dims[ndims] = {static_cast<hsize_t>(chunkSize_)};
  constexpr hsize_t  max_dims[ndims] = {H5S_UNLIMITED};
  auto space = hdf5::Dataspace::create_simple (ndims, dims, max_dims); 
  auto prop   = hdf5::Property::create();
  prop.set_chunk(ndims, chunk_dims);
  hdf5::Dataset::create<int>(group_, EVENTS_DSNAME, space, prop);
  hdf5::Dataset::create<char>(group_, PRODUCTS_DSNAME, space, prop);
  hdf5::Dataset::create<int>(group_, OFFSETS_DSNAME, space, prop);

  const auto scalar_space  = hdf5::Dataspace::create_scalar();
  hdf5::Attribute::create<int>(group_, RUN_ANAME, scalar_space);
  hdf5::Attribute::create<int>(group_, LUMISEC_ANAME, scalar_space);
  hdf5::Attribute::create<int>(group_, COMPRESSION_LEVEL_ANAME, scalar_space);
  hdf5::Attribute::create<int>(group_, COMPRESSION_CHOICE_ANAME, scalar_space);
  constexpr hsize_t     str_dims[ndims] = {10};
  auto const attr_type = H5Tcopy (H5T_C_S1);
  H5Tset_size(attr_type, H5T_VARIABLE);
  auto const attr_space  = H5Screate(H5S_SCALAR);
  hdf5::Attribute compression = hdf5::Attribute::create<std::string>(group_,COMPRESSION_ANAME, attr_space); 
  for(auto const& s: iSerializers) {
    std::string const type(s.className());
    std::string const name(s.name());
    hdf5::Attribute prod_name = hdf5::Attribute::create<std::string>(group_, name.c_str(), attr_space); 
    prod_name.write<std::string>(type);
  }
}

std::pair<std::vector<uint32_t>, std::vector<char>> PHDFBatchEventsOutputer::writeDataProductsToOutputBuffer(SerializeStrategy const& iSerializers) const{
  //Calculate buffer size needed
  uint32_t bufferSize = 0;
  std::vector<uint32_t> offsets;
  offsets.reserve(iSerializers.size()+1);
  for(auto const& s: iSerializers) {
    auto const blobSize = s.blob().size();
    offsets.push_back(bufferSize);
    bufferSize += blobSize;
  }
  offsets.push_back(bufferSize);

  //initialize with 0
  std::vector<char> buffer(bufferSize, 0);
  
  {
    uint32_t index = 0;
    for(auto const& s: iSerializers) {
      auto offset = offsets[index++];
      std::copy(s.blob().begin(), s.blob().end(), buffer.begin()+offset );
    }
    assert(buffer.size() == offsets[index]);
  }

  if(compressionChoice_ == CompressionChoice::kEvents or compressionChoice_ == CompressionChoice::kBoth) {
    auto cBuffer  = pds::compressBuffer(0,0, compression_, compressionLevel_, buffer);

    return {std::move(offsets), std::move(cBuffer)};
  }

  return {std::move(offsets), std::move(buffer)};
}
namespace {
  class Maker : public OutputerMakerBase {
  public:
    Maker(): OutputerMakerBase("PHDFBatchEventsOutputer") {}
    
    std::unique_ptr<OutputerBase> create(unsigned int iNLanes, ConfigurationParameters const& params) const final {
      auto fileName = params.get<std::string>("fileName");
      if(not fileName) {
        std::cout<<" no file name given for PHDFBatchEventsOutputer\n";
        return {};
      }

      auto chunkSize = params.get<int>("hdfchunkSize", 10485760);
      int compressionLevel = params.get<int>("compressionLevel", 18);
      auto compressionName = params.get<std::string>("compressionAlgorithm", "ZSTD");
      auto compression = pds::toCompression(compressionName);
      if(not compression) {
        std::cout <<"unknown compression "<<compressionName<<std::endl;
        return {};
      }
      auto serializationName = params.get<std::string>("serializationAlgorithm", "ROOT");
      auto serialization = pds::toSerialization(serializationName);
      if(not serialization) {
        std::cout <<"unknown serialization "<<serializationName<<std::endl;
        return {};
      }
      auto compressionChoiceName = params.get<std::string>("compressionChoice", "Events");
      auto compressionChoice = PHDFBatchEventsOutputer::CompressionChoice::kEvents;
      if(compressionChoiceName == "Events") {
      }else if(compressionChoiceName == "Batch") {
        compressionChoice = PHDFBatchEventsOutputer::CompressionChoice::kBatch;
      }else if(compressionChoiceName == "Both") {
        compressionChoice = PHDFBatchEventsOutputer::CompressionChoice::kBoth;
      }else if(compressionChoiceName == "None") {
        compressionChoice = PHDFBatchEventsOutputer::CompressionChoice::kNone;
      } else {
        std::cout <<"Unknown compression choice "<<compressionChoiceName<<std::endl;
        return {};
      }

      auto batchSize = params.get<int>("batchSize",1);

      return std::make_unique<PHDFBatchEventsOutputer>(*fileName, iNLanes, chunkSize, *compression, compressionLevel, compressionChoice, *serialization, batchSize);
    }
  };

  Maker s_maker;
}
