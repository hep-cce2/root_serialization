#include "PDSOutputer.h"
#include "summarize_serializers.h"
#include "lz4.h"
#include <iostream>
#include <cstring>
#include <set>

void PDSOutputer::setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) {
  auto& s = serializers_[iLaneIndex];
  s.reserve(iDPs.size());
  for(auto const& dp: iDPs) {
    s.emplace_back(dp.name(), dp.classType());
  }
}

void PDSOutputer::productReadyAsync(unsigned int iLaneIndex, DataProductRetriever const& iDataProduct, TaskHolder iCallback) const {
  auto& laneSerializers = serializers_[iLaneIndex];
  auto group = iCallback.group();
  laneSerializers[iDataProduct.index()].doWorkAsync(*group, iDataProduct.address(), std::move(iCallback));
}

void PDSOutputer::outputAsync(unsigned int iLaneIndex, EventIdentifier const& iEventID, TaskHolder iCallback) const {
  queue_.push(*iCallback.group(), [this, iEventID, iLaneIndex, callback=std::move(iCallback)]() mutable {
      const_cast<PDSOutputer*>(this)->output(iEventID, serializers_[iLaneIndex]);
      callback.doneWaiting();
    });
}

void PDSOutputer::printSummary() const  {
  summarize_serializers(serializers_);
}



void PDSOutputer::output(EventIdentifier const& iEventID, std::vector<SerializerWrapper> const& iSerializers) {
  if(firstTime_) {
    writeFileHeader(iSerializers);
    firstTime_ = false;
  }
  using namespace std::string_literals;
  
  std::cout <<"   run:"s+std::to_string(iEventID.run)+" lumi:"s+std::to_string(iEventID.lumi)+" event:"s+std::to_string(iEventID.event)+"\n"<<std::flush;
  
  writeEventHeader(iEventID);
  writeDataProducts(iSerializers);
  /*
    for(auto& s: iSerializers) {
    std::cout<<"   "s+s.name()+" size "+std::to_string(s.blob().size())+"\n" <<std::flush;
    }
  */
}

void PDSOutputer::writeFileHeader(std::vector<SerializerWrapper> const& iSerializers) {
  std::set<std::string> typeNamesSet;
  for(auto const& w: iSerializers) {
    std::string n(w.className());
    n.push_back('\0');
    auto it = typeNamesSet.insert(n);
  }
  std::vector<std::string> typeNames(typeNamesSet.begin(), typeNamesSet.end());
  typeNamesSet.clear();
  size_t nCharactersInTypeNames = 0U;
  for(auto const& n: typeNames) {
    nCharactersInTypeNames += n.size();
  }
  
  std::vector<std::pair<uint32_t, std::string>> dataProducts;
  dataProducts.reserve(iSerializers.size());
  dataProductIndices_.reserve(iSerializers.size());
  size_t nCharactersInDataProducts = 0U;
  size_t index = 0;
  for(auto const& s: iSerializers) {
    auto itFind = std::lower_bound(typeNames.begin(), typeNames.end(), s.className());
    std::string name{s.name()};
    name.push_back('\0');
    dataProducts.emplace_back(itFind - typeNames.begin(), name);
    dataProductIndices_.emplace_back(name,index++);
    //pad to 32 bit size
    while( 0 != dataProducts.back().second.size() % 4) {
      dataProducts.back().second.push_back('\0');
    }
    nCharactersInDataProducts += 4 + dataProducts.back().second.size();
  }
  
  std::array<char, 8> transitions = {'E','v','e','n','t','\0','\0','\0'};
  
  size_t bufferPosition = 0;
  std::vector<uint32_t> buffer;
  const auto nWordsInTypeNames = bytesToWords(nCharactersInTypeNames);
  buffer.resize(1+transitions.size()/4+1+nWordsInTypeNames+1+1+nCharactersInDataProducts/4);
  
  //The different record types stored
  buffer[bufferPosition++] = transitions.size()/4;
  std::memcpy(reinterpret_cast<char*>(buffer.data()+bufferPosition), transitions.data(), transitions.size());
  bufferPosition += transitions.size()/4;
  
  //The 'top level' types stored in the file
  buffer[bufferPosition++] = nWordsInTypeNames;
  
  size_t bufferPositionInChars = bufferPosition*4;
  for(auto const& t: typeNames) {
    std::memcpy(reinterpret_cast<char*>(buffer.data())+bufferPositionInChars, t.data(), t.size());
    bufferPositionInChars+=t.size();
  }
  //std::cout <<bufferPositionInChars<<" "<<bufferPosition*4<<" "<<bufferPositionInChars-bufferPosition*4<<" "<<nCharactersInTypeNames<<std::endl;
  assert(bufferPositionInChars-bufferPosition*4 == nCharactersInTypeNames);
  
  bufferPosition += nWordsInTypeNames;
  
  //Information about types that are not at the 'top level' (none for now)
  buffer[bufferPosition++] = 0;
  
  //The different data products to be stored
  buffer[bufferPosition++] = dataProducts.size();
  for(auto const& dp : dataProducts) {
    buffer[bufferPosition++] = dp.first;
    std::memcpy(reinterpret_cast<char*>(buffer.data()+bufferPosition), dp.second.data(), dp.second.size());
    assert(0 == dp.second.size() % 4);
    bufferPosition += dp.second.size()/4;
  }
  
  {
    //The file type identifier
    const uint32_t id = 3141592*256+1;
    file_.write(reinterpret_cast<char const*>(&id), 4);
  }
  {
    //The 'unique' file id, just dummy for now
    const uint32_t fileID = 0;
    file_.write(reinterpret_cast<char const*>(&fileID), 4);     
  }
  
  //The size of the header buffer in words (excluding first 3 words)
  const uint32_t bufferSize = buffer.size();
  file_.write(reinterpret_cast<char const*>(&bufferSize), 4);
  
  file_.write(reinterpret_cast<char const*>(buffer.data()), bufferSize*4);
  //for(auto v: buffer) {
  //  file_.write(reinterpret_cast<char const*>(&v), sizeof(v));
  //}
  
  //The size of the header buffer in words (excluding first 3 words)
  file_.write(reinterpret_cast<char const*>(&bufferSize), 4);
}

void PDSOutputer::writeEventHeader(EventIdentifier const& iEventID) {
  constexpr unsigned int headerBufferSizeInWords = 5;
  std::array<uint32_t,headerBufferSizeInWords> buffer;
  buffer[0] = 0; //Record index for Event
  buffer[1] = iEventID.run;
  buffer[2] = iEventID.lumi;
  buffer[3] = (iEventID.event >> 32) & 0xFFFFFFFF;
  buffer[4] = iEventID.event & 0xFFFFFFFF;
  file_.write(reinterpret_cast<char*>(buffer.data()), headerBufferSizeInWords*4);
}

void PDSOutputer::writeDataProducts(std::vector<SerializerWrapper> const& iSerializers) {
  //Calculate buffer size needed
  uint32_t bufferSize = 0;
  for(auto const& s: iSerializers) {
    bufferSize +=1+1;
    auto const blobSize = s.blob().size();
    bufferSize += bytesToWords(blobSize); //handles padding
  }
  //initialize with 0
  std::vector<uint32_t> buffer(size_t(bufferSize), 0);
  
  {
    uint32_t bufferIndex = 0;
    uint32_t dataProductIndex = 0;
    for(auto const& s: iSerializers) {
      buffer[bufferIndex++]=dataProductIndex++;
      auto const blobSize = s.blob().size();
      uint32_t sizeInWords = bytesToWords(blobSize);
      buffer[bufferIndex++]=sizeInWords;
      std::copy(s.blob().begin(), s.blob().end(), reinterpret_cast<char*>( &(*(buffer.begin()+bufferIndex)) ) );
      bufferIndex += sizeInWords;
    }
    assert(buffer.size() == bufferIndex);
  }
  auto const bound = LZ4_compressBound(buffer.size()*4);
  std::vector<uint32_t> cBuffer(bytesToWords(size_t(bound))+3, 0);
  auto const cSize = LZ4_compress_default(reinterpret_cast<char*>(&(*buffer.begin())), reinterpret_cast<char*>(&(*(cBuffer.begin()+2))), buffer.size()*4, bound);
  //std::cout <<"compressed "<<cSize<<" uncompressed "<<buffer.size()*4<<std::endl;
  //std::cout <<"compressed "<<float(cSize)/(buffer.size()*4)<<std::endl;
  uint32_t const recordSize = bytesToWords(cSize)+1;
  cBuffer[0] = recordSize;
  //Record the actual number of bytes used in the last word of the compression buffer in the lowest
  // 2 bits of the word
  cBuffer[1] = buffer.size()*4 + (cSize % 4);
  cBuffer[recordSize+1]=recordSize;
  file_.write(reinterpret_cast<char*>(cBuffer.data()), (recordSize+2)*4);
}
