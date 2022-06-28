#include <iostream>
#include "S3Outputer.h"
#include "OutputerFactory.h"
#include "UnrolledSerializerWrapper.h"
#include "FunctorTask.h"

using namespace cce::tf;

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
      buffers_.emplace_back(objPrefix_ + prod->productname(), prod);
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
      TaskHolder th(group, make_functor_task([](){}));
      TaskHolder productsDone(group, make_functor_task(
          [this, stripeOut=std::move(currentEventStripe_), callback=std::move(th)]() mutable {
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
    "  total serializer parallel time at end event: "<<serializerTime.count()<<"us\n";
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

  TaskHolder productsDoneCallback([this, cb=std::move(iCallback)]() mutable {
      if ( currentEventStripe_.events_size() == eventFlushSize_ ) {
        if(verbose_ >= 2) { std::cout << "reached event flush size "s + std::to_string(eventFlushSize_) + ", flushing\n"; }
        objstripe::EventStripe stripeOut;
        stripeOut.mutable_events()->Reserve(eventFlushSize_);
        std::swap(currentEventStripe_, stripeOut);
        return TaskHolder(*cb.group(), make_functor_task(
            [this, stripeOut=std::move(stripeOut), callback=std::move(cb)]() mutable {
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

  if ( not last ) {
    buf.buffer_.mutable_content()->append(blob);
    buf.buffer_.add_offsets(buf.buffer_.content().size());
  }
  size_t bufferNevents = buf.buffer_.offsets_size();

  // first flush when we exceed min size and have an even divisor of eventFlushSize_
  // subsequent flush when we reach productFlushSize
  // always flush when we reach eventFlushSize_ (for buffers that never get big enough)
  // flush if last call and we have something to write
  if (
      (
        (buf.info_->flushsize() == 0)
        && (buf.buffer_.content().size() > buf.info_->flushminbytes())
        && (eventFlushSize_ % bufferNevents == 0)
      )
      || (bufferNevents == buf.info_->flushsize())
      || (bufferNevents == eventFlushSize_)
      || (last && bufferNevents > 0)
      )
  {
    objstripe::ProductStripe pOut;
    pOut.mutable_offsets()->Reserve(bufferNevents);
    pOut.mutable_content()->reserve(buf.buffer_.content().size());
    pOut.set_globaloffset(buf.buffer_.globaloffset() + bufferNevents);
    std::swap(buf.buffer_, pOut);
    std::string name = buf.prefix_;
    name += std::to_string(buf.buffer_.globaloffset());
    iCallback.group()->run(
      [this, name=std::move(name), pOut=std::move(pOut), callback=std::move(iCallback)]() {
        std::string finalbuf;
        pOut.SerializeToString(&finalbuf);
        conn_->put(name, std::move(finalbuf), [name=std::move(name), callback=std::move(callback)](S3Request* req) {
            if ( req->status != S3Request::Status::ok ) {
              std::cerr << "failed to write product buffer " << name << std::endl;
            }
          });
      }
    );
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
  auto dest = index_.add_packedeventstripes();
  // TODO: compression
  stripe.SerializeToString(dest);
  if ( verbose_ >= 2 ) {
    std::cout << "length of packed EventStripe: " << dest->size() << "\n";
    std::cout << stripe.DebugString() << "\n";
  }

  // TODO: checkpoint only every few event stripes?
  iCallback.group()->run(
    // bind shallow copy of index_ to ensure validity
    [this, idxcopy=index_, callback=std::move(iCallback)]() {
      std::string indexOut;
      idxcopy.SerializeToString(&indexOut);
      conn_->put(objPrefix_ + "index", std::move(indexOut), [callback=std::move(callback)](S3Request* req) {
          if ( req->status != S3Request::Status::ok ) {
            std::cerr << "failed to write product buffer index" << std::endl;
          }
        });
    });
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
      auto productFlush = params.get<size_t>("productFlush", 1024*512);
      auto eventFlush = params.get<size_t>("eventFlush", 24);
      auto connfile = params.get<std::string>("conn");
      if(not connfile) {
        std::cerr <<"no connection configuration file name given for S3Outputer\n";
        return {};
      }
      auto conn = S3Connection::from_config(connfile.value()); 
      if(not conn) {
        return {};
      }

      return std::make_unique<S3Outputer>(iNLanes, objPrefix.value(), verbose, productFlush, eventFlush, conn);
    }
};

Maker s_maker;
}
