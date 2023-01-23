#include "pds_reading.h"
#include <cassert>
#include <array>
#include <algorithm>
#include <iostream>

#include "lz4.h"
#include "zstd.h"

#include "TClass.h"
#include "TBufferFile.h"

using namespace cce::tf::pds;

namespace {
  Compression whichCompression(const char* iName) {
  if(iName[0] == 'N') {
    return Compression::kNone;
  }
  if(iName[0] == 'L') {
    return Compression::kLZ4;
  }
  if(iName[0] == 'Z') {
    return Compression::kZSTD;
  }
  assert(false);
  return Compression::kNone;
}

  struct Preamble {
    uint32_t bufferSize;
    Compression compression;
    Serialization serialization;
  };
Preamble readPreamble(std::istream& iFile) {
  std::array<uint32_t, 4> header;
  iFile.read(reinterpret_cast<char*>(header.data()),4*4);
  assert(iFile.rdstate() == std::ios_base::goodbit);

  assert(3141592*256+1 == header[0] or 3141592*256+2 == header[0]);
  Serialization serialization = (header[0] -3141592*256-1) == 0? Serialization::kRoot : Serialization::kRootUnrolled; 
  return {header[3], whichCompression(reinterpret_cast<const char*>(&header[2])), serialization};
}

using buffer_iterator = std::vector<std::uint32_t>::const_iterator;
using table_iterator = std::vector<std::uint32_t>::const_iterator;

std::vector<std::string> readStringsArray(buffer_iterator& itBuffer, buffer_iterator itEnd) {
  assert(itBuffer!=itEnd);
  auto bufferSize = *(itBuffer++);

  std::vector<std::string> toReturn;
  if(bufferSize == 0) {
    return toReturn;
  }

  assert(itBuffer+bufferSize <= itEnd);

  const char* itChars = reinterpret_cast<const char*>(&(*itBuffer));
  itBuffer = itBuffer+bufferSize;

  auto itEndChars = itChars + bufferSize*4;
  auto nStrings = std::count(itChars, itEndChars,'\0');
  toReturn.reserve(nStrings);

  while(itChars != itEndChars) {
    std::string s(&(*itChars));
    if(s.empty()) {
      break;
    }
    itChars = itChars+s.size()+1;
    //std::cout <<s<<std::endl;
    toReturn.emplace_back(std::move(s));
  }
  //std::cout << (void*)itChars << " "<<&(*itBuffer)<<std::endl;
  assert(itChars <= reinterpret_cast<const char*>(&(*itBuffer)));
  return toReturn;
}

std::vector<std::string> readRecordNames(buffer_iterator& itBuffer, buffer_iterator itEnd) {
  return readStringsArray(itBuffer, itEnd);
}

std::vector<std::string> readTypes(buffer_iterator& itBuffer, buffer_iterator itEnd) {
  return readStringsArray(itBuffer, itEnd);
}

size_t bytesToWords(size_t nBytes) {
  return nBytes/4 + ( (nBytes % 4) == 0 ? 0 : 1);
}

  std::vector<ProductInfo> readProductInfo(buffer_iterator& itBuffer, buffer_iterator itEnd, std::vector<std::string> const& iClassNames) {
  assert(itBuffer != itEnd);
  //should be a loop over records, but we only have 1 for now
  auto nDataProducts = *(itBuffer++);
  std::vector<ProductInfo> info;
  info.reserve(nDataProducts);

  for(int i=0; i< nDataProducts; ++i) {
    assert(itBuffer < itEnd);
    auto classIndex = *(itBuffer++);
    assert(itBuffer < itEnd);

    const char* itChars = reinterpret_cast<const char*>(&(*itBuffer));
    std::string name(itChars);
    itBuffer = itBuffer + bytesToWords(name.size()+1);
    assert(itBuffer <= itEnd);
    //std::cout <<name <<" "<<classIndex<<std::endl;
    info.emplace_back(std::move(name), classIndex, iClassNames[classIndex]);
  }

  return info;
}

}

using namespace cce::tf;

uint32_t pds::readword(std::istream& iFile) {
  int32_t word;
  iFile.read(reinterpret_cast<char*>(&word), 4);
  assert(iFile.rdstate() == std::ios_base::goodbit);
  return word;
}

uint32_t pds::readwordNoCheck(std::istream& iFile) {
  int32_t word;
  iFile.read(reinterpret_cast<char*>(&word), 4);
  return word;
}

std::vector<uint32_t> pds::readWords(std::istream& iFile, uint32_t bufferSize) {
  std::vector<uint32_t> words(bufferSize);
  iFile.read(reinterpret_cast<char*>(words.data()), 4*bufferSize);
  assert(iFile.rdstate() == std::ios_base::goodbit);
  return words;
}

std::vector<ProductInfo> pds::readFileHeader(std::istream& file, Compression& compression, Serialization& serialization) {
  auto preamble = readPreamble(file);
  auto bufferSize = preamble.bufferSize;
  compression = preamble.compression;
  serialization = preamble.serialization;

  //1 word beyond the buffer is the crosscheck value
  std::vector<uint32_t> buffer = readWords(file, bufferSize+1);
  auto itBuffer = buffer.cbegin();
  auto itEnd = buffer.cend();

  (void) readRecordNames(itBuffer, itEnd);
  auto types = readTypes(itBuffer, itEnd);
  //non-top level types
  readTypes(itBuffer, itEnd);
  auto productInfo = readProductInfo(itBuffer, itEnd, types);
  assert(itBuffer != itEnd);
  assert(itBuffer+1 == itEnd);
  //std::cout <<*itBuffer <<" "<<bufferSize<<std::endl;
  assert(*itBuffer == bufferSize);
  return productInfo;
}

bool pds::readCompressedEventBuffer(std::istream&file, EventIdentifier& iEventID, std::vector<uint32_t>& buffer) {
  //header structure in words
  //constexpr size_t kTransitionTypeW=0;
  constexpr size_t kEventIDMSW=3;
  constexpr size_t kEventIDLSW=4;
  constexpr size_t kRunIDW=1;
  constexpr size_t kLumiIDW=2;

  //std::cout <<"readEventContent"<<std::endl;
  std::array<uint32_t, kEventHeaderSizeInWords+1> headerBuffer;
  file.read(reinterpret_cast<char*>(headerBuffer.data()), (kEventHeaderSizeInWords+1)*4);
  if( file.rdstate() & std::ios_base::eofbit) {
    return false;
  }
  assert(file.rdstate() == std::ios_base::goodbit);

  int32_t bufferSize = headerBuffer[kEventHeaderSizeInWords];

  unsigned long long eventIDTopWord = headerBuffer[kEventIDMSW];
  eventIDTopWord = eventIDTopWord <<32;
  unsigned long long eventID = eventIDTopWord+headerBuffer[kEventIDLSW];
  iEventID = {headerBuffer[kRunIDW], headerBuffer[kLumiIDW], eventID};

  buffer = readWords(file, bufferSize+1);

  int32_t crossCheckBufferSize = buffer[bufferSize];
  //std::cout <<bufferSize<<" "<<crossCheckBufferSize<<std::endl;
  assert(crossCheckBufferSize == bufferSize);
  return true;
}


std::vector<uint32_t> pds::uncompressEventBuffer(pds::Compression compression, std::vector<uint32_t> const& buffer) {
  int32_t bufferSize = buffer.size();
  //lower 2 bits are the number of bytes used in the last word of the compressed sized
  int32_t uncompressedBufferSize = buffer[0]/4;
  int32_t bytesInLastWord = buffer[0] % 4;
  int32_t compressedBufferSizeInBytes = (bufferSize-1)*4 + (bytesInLastWord == 0? 0 : (-4+bytesInLastWord));
  //std::cout <<"compressed "<<compressedBufferSizeInBytes <<" uncompressed "<<uncompressedBufferSize*4<<" extra bytes "<<bytesInLastWord<<std::endl;
  std::vector<uint32_t> uBuffer(size_t(uncompressedBufferSize), 0);
  if(Compression::kLZ4 == compression) {
    LZ4_decompress_safe(reinterpret_cast<char const*>(&(*(buffer.begin()+1))), reinterpret_cast<char*>(uBuffer.data()),
                        compressedBufferSizeInBytes,
                        uncompressedBufferSize*4);
  } else if(Compression::kZSTD == compression) {
    ZSTD_decompress(uBuffer.data(), uncompressedBufferSize*4, &(*(buffer.begin()+1)), compressedBufferSizeInBytes);
  } else if(Compression::kNone == compression) {
    assert(buffer.size() == uBuffer.size()+1);
    std::copy(buffer.begin()+1, buffer.begin()+buffer.size(), uBuffer.begin());
  }
  return uBuffer;
}

void pds::deserializeDataProducts(buffer_iterator it, buffer_iterator itEnd, std::vector<DataProductRetriever>& dataProducts, DeserializeStrategy const& deserializers) {

  while(it < itEnd) {
    auto productIndex = *(it++);
    auto storedSize = *(it++);
    //std::cout <<" deserialize "<<productIndex<<" "<<storedSize<<std::endl;

    //std::cout <<dataProducts[productIndex].name()<<" "<<dataProducts[productIndex].classType()->GetName()<<std::endl;
    //std::cout <<"storedSize "<<storedSize<<" "<<storedSize*4<<std::endl;
    auto readSize = deserializers[productIndex].deserialize(reinterpret_cast<char const*>(&*it), storedSize*4, *dataProducts[productIndex].address());
    dataProducts[productIndex].setSize(readSize);
    //std::cout <<" readSize "<<readSize<<"\n";

    it = it+storedSize;
    //std::cout <<itEnd - it<<std::endl;
  }
  assert(it==itEnd);
}


std::vector<char> pds::uncompressBuffer(pds::Compression compression, std::vector<char> const& buffer, uint32_t uncompressedBufferSize) {
  std::vector<char> uBuffer(size_t(uncompressedBufferSize), 0);
  if(Compression::kLZ4 == compression) {
    auto size = LZ4_decompress_safe(&(*(buffer.begin())), uBuffer.data(),
                                    buffer.size(),
                                    uncompressedBufferSize);
    if(size != uncompressedBufferSize) {
      if(size > 0) {
        std::cout <<" LZ4_decompress_safe decompressed less bytes ("<<size<<") than expected ("<<uncompressedBufferSize<<")"<<std::endl;
      } else {
        throw std::runtime_error("LZ4_decompress_safe failed to decompress");
      }
    }
    assert(size == uncompressedBufferSize);
  } else if(Compression::kZSTD == compression) {
    ZSTD_decompress(uBuffer.data(), uncompressedBufferSize, &(*(buffer.begin())), buffer.size());
  } else if(Compression::kNone == compression) {
    assert(buffer.size() == uBuffer.size());
    std::copy(buffer.begin(), buffer.begin()+buffer.size(), uBuffer.begin());
  }
  return uBuffer;
}

void pds::deserializeDataProducts(const char* it, const char* itEnd, 
                                  table_iterator itTable, table_iterator itTableEnd,
                                  std::vector<DataProductRetriever>& dataProducts, DeserializeStrategy const& deserializers) {

  auto itBegin = it;
  uint32_t productIndex = 0;
  while(it < itEnd and itTable != itTableEnd) {
    auto start = *itTable;
    auto next = *(++itTable);
    auto storedSize = next - start;
    if( storedSize != 0) {
      //std::cout <<" deserialize "<<productIndex<<" "<<storedSize<<std::endl;

      //std::cout <<dataProducts[productIndex].name()<<" "<<dataProducts[productIndex].classType()->GetName()<<std::endl;
      //std::cout <<"storedSize "<<storedSize<<" "<<storedSize*4<<std::endl;
      auto readSize = deserializers[productIndex].deserialize(it, storedSize, *dataProducts[productIndex].address());
      dataProducts[productIndex].setSize(readSize);
      //std::cout <<" readSize "<<readSize<<"\n";

      it = itBegin + next;
      //std::cout <<itEnd - it<<std::endl;
    }
    ++productIndex;
  }
  assert(it==itEnd);
}


bool pds::skipToNextEvent(std::istream& iFile) {
  iFile.seekg(kEventHeaderSizeInWords*4, std::ios_base::cur);
  if( iFile.rdstate() & std::ios_base::eofbit) {
    return false;
  }
  assert(iFile.rdstate() == std::ios_base::goodbit);

  int32_t bufferSize = readwordNoCheck(iFile);
  if( iFile.rdstate() & std::ios_base::eofbit) {
    return false;
  }

  iFile.seekg(bufferSize*4,std::ios_base::cur);
  assert(iFile.rdstate() == std::ios_base::goodbit);

  int32_t crossCheckBufferSize = readword(iFile);
  assert(crossCheckBufferSize == bufferSize);

  return true;
}
