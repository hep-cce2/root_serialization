#include <iostream>
#include "S3Outputer.h"
#include "OutputerFactory.h"
#include "UnrolledSerializerWrapper.h"
#include "FunctorTask.h"

#if ZSTD_VERSION_NUMBER < (1*100*100 + 3*100)
#error("zstd is too old")
#endif

using namespace cce::tf;

namespace {
size_t zstd_compress(ZSTD_CCtx* ctx, const std::string_view blob, std::string& out, bool flush) {
  size_t tail{out.size()};
  if ( out.capacity() < ZSTD_CStreamOutSize() ) out.resize(ZSTD_CStreamOutSize());
  else out.resize(out.capacity());
  ZSTD_outBuffer_s obuf{.dst=out.data(), .size=out.size(), .pos=tail};

  size_t status;
  if ( flush ) {
    ZSTD_inBuffer_s ibuf{.src=nullptr, .size=0, .pos=0};
    do {
      if ( obuf.pos == obuf.size ) {
        size_t new_size = (obuf.size * 3) / 2;
        out.resize(new_size);
        obuf.dst = out.data();
        obuf.size = new_size;
      }
      status = ZSTD_compressStream2(ctx, &obuf, &ibuf, ZSTD_e_end);
      if ( ZSTD_isError(status) ) {
        std::cerr <<"ERROR in compression " << ZSTD_getErrorName(status) << std::endl;
      }
    } while ( status != 0 );
  } else {
    ZSTD_inBuffer_s ibuf{.src=blob.data(), .size=blob.size(), .pos=0};
    while ( ibuf.pos < ibuf.size ) {
      status = ZSTD_compressStream2(ctx, &obuf, &ibuf, ZSTD_e_continue);
      if ( ZSTD_isError(status) ) {
        std::cerr <<"ERROR in compression " << ZSTD_getErrorName(status) << std::endl;
      }
      if ( obuf.pos == obuf.size ) {
        size_t new_size = obuf.size * 2;
        out.resize(new_size);
        obuf.dst = out.data();
        obuf.size = new_size;
      }
    }
  }
  out.resize(obuf.pos);
  // we are supposed to get a hint from ZSTD of the bytes left in internal buffers of CCtx
  // but it doesn't appear to be nonzero
  return status;
}

lzma_ret lzma_init(lzma_stream* strm, uint32_t level) {
  lzma_options_lzma opt_lzma2;
  lzma_filter filters[] = {
    { .id = LZMA_FILTER_LZMA2, .options = &opt_lzma2 },
    { .id = LZMA_VLI_UNKNOWN,  .options = NULL },
  };
  lzma_lzma_preset(&opt_lzma2, level);
  // TODO: pass through target stripe size for better choice of dict size?
  // ROOT choice: input size / 4
  opt_lzma2.dict_size = std::max(LZMA_DICT_SIZE_MIN, 32768u);
  return lzma_stream_encoder(strm, filters, LZMA_CHECK_CRC32);
}

size_t lzma_compress(lzma_stream* strm, const std::string_view blob, std::string& out, bool flush) {
  size_t tail{out.size()};
  if ( out.capacity() < BUFSIZ ) out.resize(BUFSIZ);
  else out.resize(out.capacity());

  lzma_action action = LZMA_RUN;
  strm->next_out = (uint8_t*) out.data() + tail;
  strm->avail_out = out.size() - tail;
  if ( flush ) {
    action = LZMA_FINISH;
    strm->next_in = NULL;
    strm->avail_in = 0;
  } else {
    strm->next_in = (const uint8_t*) blob.data();
    strm->avail_in = blob.size();
  }

  while ( (strm->avail_in > 0) || flush ) {
    lzma_ret ret = lzma_code(strm, action);
    if ( ret == LZMA_STREAM_END ) break;
    else if ( strm->avail_out == 0 ) {
      size_t old_size = out.size();
      size_t new_size = (old_size * 3) / 2;
      out.resize(new_size);
      strm->next_out = (uint8_t*) out.data() + old_size;
      strm->avail_out = new_size - old_size;
    }
    else if (ret != LZMA_OK) {
      std::cerr << "ERROR in lzma compression " << ret << std::endl;
      break;
    }
  }

  out.resize(out.size() - strm->avail_out);
  return 0;
}
} // anonymous namespace

StreamCompressor::StreamCompressor(const objstripe::Compression& setting):
  setting_{setting}
{
  switch ( setting_.type() ) {
    case objstripe::CompressionType::kNone:
      break;
    case objstripe::CompressionType::kZSTD:
      zstd_.reset(ZSTD_createCStream());
      ZSTD_CCtx_setParameter(zstd_.get(), ZSTD_c_compressionLevel, setting_.level());
      break;
    case objstripe::CompressionType::kLZMA:
      lzma_.reset((lzma_stream*) malloc(sizeof(lzma_stream)));
      memset(lzma_.get(), 0, sizeof(lzma_stream));
      lzma_ret ret = ::lzma_init(lzma_.get(), setting_.level());
      if (ret != LZMA_OK) { throw std::runtime_error("Could not initialize LZMA encoder: " + std::to_string(ret)); }
      break;
  }
}

size_t StreamCompressor::write(const std::string_view blob, std::string& out) {
  switch ( setting_.type() ) {
    case objstripe::CompressionType::kNone:
      out.append(blob);
      return 0;
    case objstripe::CompressionType::kZSTD:
      return ::zstd_compress(zstd_.get(), blob, out, false);
    case objstripe::CompressionType::kLZMA:
      return ::lzma_compress(lzma_.get(), blob, out, false);
    default:
      assert(false);
      return 0;
  }
}

void StreamCompressor::flush(std::string& out) {
  switch ( setting_.type() ) {
    case objstripe::CompressionType::kNone:
      return;
    case objstripe::CompressionType::kZSTD:
      ::zstd_compress(zstd_.get(), {}, out, true);
      return;
    case objstripe::CompressionType::kLZMA:
      ::lzma_compress(lzma_.get(), {}, out, true);
      // unlike zstd, lzma must be (TODO: true?) reset after each finish
      if (
          ::lzma_init(lzma_.get(), setting_.level()) != LZMA_OK
          ) { throw std::runtime_error("Could not initialize LZMA encoder"); }
      return;
    default:
      assert(false);
      return;
  }
}

void S3Outputer::setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) {
  auto& s = serializers_[iLaneIndex];
  switch(index_.serializestrategy()) {
    case objstripe::SerializeStrategy::kRoot:
      s = SerializeStrategy::make<SerializeProxy<SerializerWrapper>>();
      break;
    case objstripe::SerializeStrategy::kRootUnrolled:
      s = SerializeStrategy::make<SerializeProxy<UnrolledSerializerWrapper>>();
      break;
    default:
      throw std::runtime_error("S3Outputer: unrecognized serialization strategy");
  }
  s.reserve(iDPs.size());
  for(auto const& dp: iDPs) {
    s.emplace_back(dp.name(), dp.classType());
  }
  if (buffers_.size() == 0) {
    buffers_.reserve(iDPs.size());
    index_.mutable_products()->Reserve(iDPs.size());
    for(auto const& ss: s) {
      auto* prod = index_.add_products();
      prod->set_productname(std::string(ss.name()));
      prod->set_producttype(ss.className());
      prod->set_flushsize(0);
      prod->set_flushminbytes(productBufferFlushMinBytes_);
      // TODO: choose compression setting based on properties of ss?
      buffers_.emplace_back(objPrefix_ + "/" + prod->productname(), prod, defaultCompression_);
    }
  }
  // all lanes see same products? if not we'll need a map
  assert(buffers_.size() == iDPs.size());
}

void S3Outputer::productReadyAsync(unsigned int iLaneIndex, DataProductRetriever const& iDataProduct, TaskHolder iCallback) const {
  assert(iLaneIndex < serializers_.size());
  auto& laneSerializers = serializers_[iLaneIndex];
  auto group = iCallback.group();
  assert(iDataProduct.index() < laneSerializers.size() );
  laneSerializers[iDataProduct.index()].doWorkAsync(*group, iDataProduct.address(), std::move(iCallback));
}

void S3Outputer::outputAsync(unsigned int iLaneIndex, EventIdentifier const& iEventID, TaskHolder iCallback) const {
  auto start = std::chrono::high_resolution_clock::now();
  auto group = iCallback.group();
  collateQueue_.push(*group, [this, iEventID, iLaneIndex, callback=std::move(iCallback)]() mutable {
      collateProducts(iEventID, serializers_[iLaneIndex], std::move(callback));
    });
  auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
  parallelTime_ += time.count();
}

void S3Outputer::printSummary() const {
  {
    tbb::task_group group;
    {
      TaskHolder finalTask(group, make_functor_task([&group, task=group.defer([](){})]() mutable { group.run(std::move(task)); }));
      TaskHolder productsDone(group, make_functor_task(
          [this, stripeOut=std::move(currentEventStripe_), callback=std::move(finalTask)]() mutable {
            flushQueue_.push(*callback.group(), [this, stripeOut=std::move(stripeOut), callback=std::move(callback)]() {
                flushEventStripe(stripeOut, std::move(callback), true);
              });
          }
        ));
      for(auto& buf : buffers_) {
        buf.appendQueue_.push(group, [this, &buf, cb=productsDone]() mutable {
            appendProductBuffer(buf, {}, std::move(cb), true);
          });
      }
    }
    group.wait();
  }

  if(verbose_ >= 2) {
    summarize_serializers(serializers_);
  }
  std::chrono::microseconds serializerTime = std::chrono::microseconds::zero();
  for(const auto& lane : serializers_) {
    for(const auto& s : lane) {
      serializerTime += s.accumulatedTime();
    }
  }
  std::chrono::microseconds appendTime = std::chrono::microseconds::zero();
  for(const auto& buf : buffers_) {
    appendTime += buf.appendTime_;
  }
  std::cout <<"S3Outputer\n"
    "  total serial collate time at end event: "<<collateTime_.count()<<"us\n"
    "  total serial event stripe flush time at end event: "<<flushTime_.count()<<"us\n"
    "  total per-product serial buffer time at end event: "<<appendTime.count()<<"us\n"
    "  total non-serializer parallel time at end event: "<<parallelTime_.load()<<"us\n"
    "  total serializer parallel time at end event: "<<serializerTime.count()<<"us\n"
    "  total blocking time in S3Connection: "<<conn_->blockingTime().count()<<"us\n";
}

void S3Outputer::collateProducts(
    EventIdentifier const& iEventID,
    SerializeStrategy const& iSerializers,
    TaskHolder iCallback
    ) const
{
  using namespace std::string_literals;
  auto start = std::chrono::high_resolution_clock::now();
  auto sev = currentEventStripe_.add_events();
  sev->set_offset(eventGlobalOffset_++);
  sev->set_run(iEventID.run);
  sev->set_lumi(iEventID.lumi);
  sev->set_event(iEventID.event);
  if (verbose_ >= 2) { std::cout << sev->DebugString(); }

  TaskHolder productsDoneCallback(
    // make lambda and call, since move assignment is disabled
    // (copy callback so it lasts duration of this scope)
    [this, cb=iCallback]() mutable {
      if ( currentEventStripe_.events_size() == eventFlushSize_ ) {
        objstripe::EventStripe stripeOut;
        stripeOut.mutable_events()->Reserve(eventFlushSize_);
        std::swap(currentEventStripe_, stripeOut);
        std::cerr << "flush " + std::to_string(numFireAndForgetCollates_)
            + " " + std::to_string(std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1))
            + "\n";
        auto nextCallback = ( numFireAndForgetCollates_ < maxFireAndForgetCollates_ ) ?
          numFireAndForgetCollates_++, TaskHolder(*cb.group(), make_functor_task([this]() mutable {numFireAndForgetCollates_--;})) : std::move(cb);
        return TaskHolder(*nextCallback.group(), make_functor_task(
            [this, stripeOut=std::move(stripeOut), callback=std::move(nextCallback)]() mutable {
              if(verbose_ >= 2) { std::cout << "reached event flush size "s + std::to_string(eventFlushSize_) + ", flushing\n"; }
              flushQueue_.push(*callback.group(), [this, stripeOut=std::move(stripeOut), callback=std::move(callback)]() {
                  flushEventStripe(stripeOut, std::move(callback));
                });
            }
          ));
      }
      return cb;
    }()
  );

  auto buf = std::begin(buffers_);
  for (const auto& s : iSerializers) {
    const std::string_view blob(s.blob().data(), s.blob().size());
    buf->appendQueue_.push(*productsDoneCallback.group(), [this, buf, blob, cb=productsDoneCallback]() mutable {
        appendProductBuffer(*buf, blob, std::move(cb));
      });
    buf++;
  }
  collateTime_ += std::chrono::duration_cast<decltype(collateTime_)>(std::chrono::high_resolution_clock::now() - start);
}

void S3Outputer::appendProductBuffer(
    ProductOutputBuffer& buf,
    const std::string_view blob,
    TaskHolder iCallback,
    bool last
    ) const
{
  using namespace std::string_literals;
  auto start = std::chrono::high_resolution_clock::now();

  size_t pendingbytes{0};
  if ( not last ) {
    buf.stripe_.add_counts(blob.size());
    pendingbytes = buf.compressor_.write(blob, *buf.stripe_.mutable_content());
  }
  const size_t bufferNevents = buf.stripe_.counts_size();
  size_t bufferNbytes = buf.stripe_.content().size();
  if ( pendingbytes > 0 ) {
    std::cout << "product buffer for "s + std::string(buf.info_->productname())
      + " put " + std::to_string(blob.size()) + " bytes in"
      " and has "s + std::to_string(bufferNbytes) + " bytes out"
      " and "s + std::to_string(pendingbytes) + " bytes pending\n";
  }

  // first flush when we exceed min size and have an even divisor of eventFlushSize_
  // subsequent flush when we reach productFlushSize
  // always flush when we reach eventFlushSize_ (for buffers that never get big enough)
  // flush if last call and we have something to write
  if (
      (
        (buf.info_->flushsize() == 0)
        && (bufferNbytes > buf.info_->flushminbytes())
        && (eventFlushSize_ % bufferNevents == 0)
      )
      || (bufferNevents == buf.info_->flushsize())
      || (bufferNevents == eventFlushSize_)
      || (last && bufferNevents > 0)
      )
  {
    buf.compressor_.flush(*buf.stripe_.mutable_content());
    bufferNbytes = buf.stripe_.content().size();
    if(verbose_ >= 2) {
      std::cout << "product buffer for "s + std::string(buf.info_->productname())
        + " is full ("s + std::to_string(bufferNbytes)
        + " bytes, "s + std::to_string(bufferNevents) + " events), flushing\n";
    }

    std::string name = buf.prefix_ + "/" + std::to_string(buf.stripe_.globaloffset());
    auto req = std::make_shared<S3Request>(S3Request::Type::put, name);
    buf.stripe_.SerializeToString(&req->buffer);
    auto putDoneTask = TaskHolder(*iCallback.group(), make_functor_task([req, callback=std::move(iCallback)]() {
        if ( req->status != S3Request::Status::ok ) {
          std::cerr << "failed to write product buffer " << *req << std::endl;
        }
      }));
    conn_->submit(std::move(req), std::move(putDoneTask));

    buf.stripe_.clear_counts();
    buf.stripe_.clear_content();
    buf.stripe_.set_globaloffset(buf.stripe_.globaloffset() + bufferNevents);
    if ( buf.info_->flushsize() == 0 ) {
      // only modification to info_, done inside serial appendQueue_
      buf.info_->set_flushsize(bufferNevents);
    }
  }
  buf.appendTime_ += std::chrono::duration_cast<decltype(buf.appendTime_)>(std::chrono::high_resolution_clock::now() - start);
}

void S3Outputer::flushEventStripe(const objstripe::EventStripe& stripe, TaskHolder iCallback, bool last) const {
  if ( last and stripe.events_size() == 0 ) {
    return;
  }
  auto start = std::chrono::high_resolution_clock::now();
  index_.set_totalevents(index_.totalevents() + stripe.events_size());
  {
    auto dest = index_.add_packedeventstripes();
    std::string buf;
    stripe.SerializeToString(&buf);
    eventStripeCompressor_.write(buf, *dest);
    eventStripeCompressor_.flush(*dest);
    index_.add_eventstripesizes(buf.size());
    if ( verbose_ >= 2 ) {
      std::cout << "length of packed EventStripe: " << dest->size() << "\n";
    }
  }

  // TODO: checkpoint only every few event stripes?
  auto req = std::make_shared<S3Request>(S3Request::Type::put, "index/" + objPrefix_);
  index_.SerializeToString(&req->buffer);
  auto putDoneTask = TaskHolder(*iCallback.group(), make_functor_task([req, callback=std::move(iCallback)]() {
      if ( req->status != S3Request::Status::ok ) {
        std::cerr << "failed to write product buffer index" << std::endl;
        // TODO: if several failures, maybe exit?
      }
    }));
  conn_->submit(std::move(req), std::move(putDoneTask));
  flushTime_ += std::chrono::duration_cast<decltype(flushTime_)>(std::chrono::high_resolution_clock::now() - start);
}

namespace {
class Maker : public OutputerMakerBase {
  public:
    Maker(): OutputerMakerBase("S3Outputer") {}
    std::unique_ptr<OutputerBase> create(unsigned int iNLanes, ConfigurationParameters const& params) const final {
      auto verbose = params.get<int>("verbose", 0);
      auto objPrefix = params.get<std::string>("prefix");
      if(not objPrefix) {
        std::cerr << "no object prefix given for S3Outputer\n";
        return {};
      }
      auto productFlush = params.get<size_t>("productFlush", 1024*128);
      auto eventFlush = params.get<size_t>("eventFlush", 144);
      auto connfile = params.get<std::string>("conn");
      if(not connfile) {
        std::cerr <<"no connection configuration file name given for S3Outputer\n";
        return {};
      }
      auto conn = S3Connection::from_config(connfile.value()); 
      if(not conn) {
        return {};
      }
      auto cType = objstripe::CompressionType::kZSTD;
      uint32_t cLevel = 4;
      auto cTypeStr = params.get<std::string>("compression");
      if(cTypeStr) {
        if ( cTypeStr.value() == "ZSTD" ) {
          cType = objstripe::CompressionType::kZSTD;
        }
        else if ( cTypeStr.value() == "LZMA" ) {
          cType = objstripe::CompressionType::kLZMA;
          cLevel = 9;
        }
        else {
          std::cerr << "Unrecognized compression type: " << cTypeStr.value() << "\n";
          return {};
        }
      }

      return std::make_unique<S3Outputer>(iNLanes, objPrefix.value(), verbose, productFlush, eventFlush, conn, cType, cLevel);
    }
};

Maker s_maker;
}
