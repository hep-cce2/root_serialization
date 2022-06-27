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
  if (currentProductStripes_.size() == 0) {
    currentProductStripes_.resize(iDPs.size());
    index_.mutable_products()->Reserve(iDPs.size());
    for(auto const& ss: s) {
      auto prod = index_.add_products();
      prod->set_productname(std::string(ss.name()));
      prod->set_producttype(ss.className());
      prod->set_flushsize(0);
    }
  }
  // all lanes see same products? if not we'll need a map
  assert(currentProductStripes_.size() == iDPs.size());
}

void S3Outputer::printSummary() const {
  {
    tbb::task_group group;
    {
      auto start = std::chrono::high_resolution_clock::now();
      TaskHolder th(group, make_functor_task([](){}));
      flushProductStripes(th, true);
      flushEventStripe(th, true);
      serialTime_ += std::chrono::duration_cast<decltype(serialTime_)>(std::chrono::high_resolution_clock::now() - start);
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
  std::cout <<"S3Outputer\n  total serial time at end event: "<<serialTime_.count()<<"us\n"
    "  total non-serializer parallel time at end event: "<<parallelTime_.load()<<"us\n"
    "  total serializer parallel time at end event: "<<serializerTime.count()<<"us\n";
}
void S3Outputer::output(
    EventIdentifier const& iEventID,
    SerializeStrategy const& iSerializers,
    TaskHolder iCallback
    ) const
{
  using namespace std::string_literals;
  if(verbose_ >= 2) {
    std::cout <<"   run:"s+std::to_string(iEventID.run)+" lumi:"s+std::to_string(iEventID.lumi)+" event:"s+std::to_string(iEventID.event)+"\n"<<std::flush;
  }
  auto sev = currentEventStripe_.add_events();
  sev->set_offset(eventGlobalOffset_++);
  sev->set_run(iEventID.run);
  sev->set_lumi(iEventID.lumi);
  sev->set_event(iEventID.event);

  auto p = std::begin(currentProductStripes_);
  auto pi = index_.mutable_products()->begin();
  for(const auto& s : iSerializers) {
    if (verbose_ >= 2) {
      std::cout << "adding blob len " << s.blob().size() << " to " << pi->productname() << "\n";
      for (auto c : s.blob()) {
        if ( isprint(c) ) std::cout << c;
        else std::cout << "\\x" << std::hex << (int) c << std::dec;
      }
      std::cout << "\n";
    }
    p->mutable_content()->append(s.blob().begin(), s.blob().end());
    p->add_offsets(p->content().size());
    p++; pi++;
  }

  flushProductStripes(iCallback);

  if ( currentEventStripe_.events_size() == eventFlushSize_ ) {
    if(verbose_ >= 2) {
      std::cout << "reached event flush size "s + std::to_string(eventFlushSize_) + ", flushing\n" << std::flush;
    }
    flushEventStripe(iCallback);
  }
}


void S3Outputer::flushProductStripes(TaskHolder iCallback, bool last) const {
  using namespace std::string_literals;
  auto p = currentProductStripes_.begin();
  auto pi = index_.mutable_products()->begin();
  for(; p != std::end(currentProductStripes_); ++p, ++pi) {
    size_t offset = p->content().size();
    size_t bufferNevents = p->offsets_size();

    // first flush when we exceed min size and have an even divisor of eventFlushSize_
    // subsequent flush when we reach productFlushSize
    // always flush when we reach eventFlushSize_ (for buffers that never get big enough)
    // flush if last call and we have something to write
    if (
        ((pi->flushsize() == 0) && (offset > productBufferFlushMinSize_) && (eventFlushSize_ % bufferNevents == 0))
        || (bufferNevents == pi->flushsize())
        || (bufferNevents == eventFlushSize_)
        || (last && bufferNevents > 0)
        )
    {
      if(verbose_ >= 2) {
        std::cout << "product buffer for "s + std::string(pi->productname()) + " is full ("s + std::to_string(offset)
          + " bytes, "s + std::to_string(bufferNevents) + " events), flushing\n" << std::flush;
      }
      objstripe::ProductStripe pOut;
      pOut.mutable_offsets()->Reserve(bufferNevents);
      pOut.mutable_content()->reserve(offset);
      std::swap(*p, pOut);
      std::string name = objPrefix_;
      name += pi->productname();
      name += std::to_string(eventGlobalOffset_ - bufferNevents);
      iCallback.group()->run(
        [this, name=std::move(name), pOut=std::move(pOut), callback=iCallback]() {
          auto start = std::chrono::high_resolution_clock::now();
          std::string finalbuf;
          pOut.SerializeToString(&finalbuf);
          conn_->put(name, std::move(finalbuf), [name=std::move(name), callback=std::move(callback)](S3Request* req) {
              if ( req->status != S3Request::Status::ok ) {
                std::cerr << "failed to write product buffer " << name << std::endl;
              }
            });
          auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
          parallelTime_ += time.count();
        }
      );
      if ( pi->flushsize() == 0 ) {
        pi->set_flushsize(bufferNevents);
      }
    }
  }
}

void S3Outputer::flushEventStripe(TaskHolder iCallback, bool last) const {
  if ( not last ) {
    // all buffers should be empty because the sizes all evenly divide eventFlushSize_
    for(auto& p : currentProductStripes_) {
      assert(p.offsets_size() == 0);
    }
  }
  index_.set_totalevents(eventGlobalOffset_);
  if ( last and currentEventStripe_.events_size() == 0 ) {
    return;
  }
  objstripe::EventStripe stripeOut;
  stripeOut.mutable_events()->Reserve(eventFlushSize_);
  std::swap(currentEventStripe_, stripeOut);
  // TODO: are we sure writing to dest is threadsafe?
  auto dest = index_.add_packedeventstripes();
  iCallback.group()->run(
    [this, dest, stripeOut=std::move(stripeOut), callback=iCallback]() {
      auto start = std::chrono::high_resolution_clock::now();
      // TODO: compression
      stripeOut.SerializeToString(dest);
      if ( verbose_ >= 2 ) {
        std::cout << "length of packed EventStripe: " << dest->size() << "\n";
        std::cout << stripeOut.DebugString() << "\n";
      }

      // TODO: checkpoint only every few event stripes?
      if ( verbose_ >= 2 ) {
        std::cout << index_.DebugString() << "\n";
      }
      std::string indexOut;
      index_.SerializeToString(&indexOut);
      conn_->put(objPrefix_ + "index", std::move(indexOut), [callback=std::move(callback)](S3Request* req) {
          if ( req->status != S3Request::Status::ok ) {
            std::cerr << "failed to write product buffer index" << std::endl;
          }
        });
      auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
      parallelTime_ += time.count();
    }
  );
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
