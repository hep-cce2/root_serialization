#include "TROOT.h"
#include "TVirtualStreamerInfo.h"
#include "TObject.h"
#include <iostream>
#include <vector>
#include <string>
#include <atomic>
#include <iomanip>
#include <cmath>

#include "CLI11.hpp"

#define TBB_PREVIEW_TASK_GROUP_EXTENSIONS 1 // for task_group::defer

#include "outputerFactoryGenerator.h"
#include "sourceFactoryGenerator.h"
#include "waiterFactoryGenerator.h"

#include "Lane.h"
#include "FunctorTask.h"

#include "tbb/task_group.h"
#include "tbb/global_control.h"
#include "tbb/task_arena.h"

namespace {
  std::pair<std::string, std::string> parseCompound(std::string_view iArg) {
    std::string sArg(iArg);
    auto foundEq = sArg.find('=');
    auto foundComma = sArg.find(':');
    auto found = foundEq < foundComma ? foundEq : foundComma;
    if(found != std::string::npos) {
      return std::pair(sArg.substr(0,found), sArg.substr(found+1));
    }
    return std::pair(sArg, std::string());
  }
}

int main(int argc, char* argv[]) {
  using namespace cce::tf;
  CLI::App app{"test different I/O systems under threading"};

  std::string sourceConfig;
  app.add_option("-s,--source",sourceConfig,"configure Source")->required();
  
  int parallelism = tbb::this_task_arena::max_concurrency();
  app.add_option("-t,--num-threads", parallelism, "number of threads to use.\nDefault is all cores on the machine.");

  bool useIMT = false;
  app.add_option("--use-IMT", useIMT, "Use ROOT's Implicit MultiThreading.\nDefault is false.");

  unsigned int nLanes = parallelism;
  app.add_option("-l,--num-lanes", nLanes, "Number of concurrently processing event Lanes.\nDefault is number of threads.");

  unsigned long long nEvents = std::numeric_limits<unsigned long long>::max();
  app.add_option("-n,--num-events", nEvents, "Number of events to process.\nDefault is max value.");

  std::string outputerConfig="DummyOutputer";
  app.add_option("-o,--outputer", outputerConfig, "configure Outputer.\nDefault is 'DummyOutputer'.");

  std::string waiterConfig;
  app.add_option("-w,--waiter", waiterConfig, "configure Waiter.\nDefault is no waiter denoted by ''.");

  CLI11_PARSE(app, argc, argv);

  tbb::global_control c(tbb::global_control::max_allowed_parallelism, parallelism);

  //Tell Root we want to be multi-threaded
  if(useIMT) {
    ROOT::EnableImplicitMT(parallelism);
  } else {
    ROOT::EnableThreadSafety();
  }
  //When threading, also have to keep ROOT from logging all TObjects into a list
  TObject::SetObjectStat(false);
  
  //Have to avoid having Streamers modify themselves after they have been used
  TVirtualStreamerInfo::Optimize(false);

  std::vector<Lane> lanes;

  std::function<std::unique_ptr<OutputerBase>(unsigned int)> outFactory;
  {
    auto [outputType, outputInfo] = parseCompound(outputerConfig);
    outFactory = outputerFactoryGenerator(outputType, outputInfo);
    if(not outFactory) {
      std::cout <<"unknown output type "<<outputType<<std::endl;
      return 1;
    }
  }

  auto [sourceType, sourceOptions] = parseCompound(sourceConfig);
  auto sourceFactory = sourceFactoryGenerator(sourceType, sourceOptions);
  if(not sourceFactory) {
    std::cout <<"unknown source type "<<sourceType<<std::endl;
    return 1;
  }

  decltype(waiterFactoryGenerator(waiterConfig, waiterConfig)) waiterFactory;
  if(not waiterConfig.empty()) {
    auto [type, options] = parseCompound(waiterConfig);
    waiterFactory = waiterFactoryGenerator(type, options);
    if(not waiterFactory) {
      std::cout <<"unknown waiter type "<<type<<std::endl;
      return 1;
    }
  }

  {
    //warm up the system by processing 1 event 
    tbb::task_arena arena(1);
    auto out = outFactory(1);
    if(not out) {
      std::cout <<"failed to create outputer\n";
      return 1;
    }
    auto source =sourceFactory(1,1);
    if(not source) {
      std::cout <<"failed to create source\n";
      return 1;
    }
    Lane lane(0, source.get(), nullptr);
    out->setupForLane(0, lane.dataProducts());
    auto pOut = out.get();
    std::cout <<"begin warmup"<<std::endl;
    arena.execute([&lane,pOut]() {
        tbb::task_group group;
        std::atomic<long> ievt{0};
        TaskHolder finalTask(group, make_functor_task([&group, task=group.defer([](){})]() mutable { group.run(std::move(task)); }));
        group.run([&]() {
            lane.processEventsAsync(ievt, group, *pOut, std::move(finalTask));
          });
        group.wait();
      });
  }
  std::cout <<"finished warmup"<<std::endl;

  auto out = outFactory(nLanes);
  auto source = sourceFactory(nLanes, nEvents);
  std::unique_ptr<WaiterBase> waiter;
  if(waiterFactory) {
    waiter = waiterFactory(nLanes, source->numberOfDataProducts());
    if(not waiter) {
      std::cout <<"failed to create Waiter "<<waiterConfig<<std::endl;
      return 1;
    }
  }
  lanes.reserve(nLanes);
  for(unsigned int i = 0; i< nLanes; ++i) {
    lanes.emplace_back(i, source.get(), waiter.get());
    out->setupForLane(i, lanes.back().dataProducts());
  }

  std::atomic<long> ievt{0};
  
  tbb::task_arena arena(parallelism);

  decltype(std::chrono::high_resolution_clock::now()) start;
  auto pOut = out.get();
  arena.execute([&lanes, &ievt, pOut, &start]() {
    std::vector<tbb::task_group> groups(lanes.size());
    start = std::chrono::high_resolution_clock::now();
    auto itGroup = groups.begin();
    {
      for(auto& lane: lanes) {
        auto& group = *itGroup;
        TaskHolder finalTask(group, make_functor_task([&group, task=group.defer([](){})]() mutable { group.run(std::move(task)); }));
        group.run([&, ft=std::move(finalTask)]() {lane.processEventsAsync(ievt, group, *pOut, std::move(ft));});
        ++itGroup;
      }
    }
    //be sure all groups have fully finished
    for(auto& group: groups) {
      group.wait();
    }
  });

  std::chrono::microseconds eventTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-start);

  //NOTE: each lane will go 1 beyond the # events so ievt is more then the # events
  std::cout <<"----------"<<std::endl;
  std::cout <<"Source "<<sourceConfig<<"\n"
            <<"Outputer "<<outputerConfig<<"\n"
	    <<"Waiter "<<waiterConfig<<"\n"
	    <<"# threads "<<parallelism<<"\n"
	    <<"# concurrent events "<<nLanes <<"\n"
	    <<"use ROOT IMT "<< (useIMT? "true\n":"false\n");
  std::cout <<"Event processing time: "<<eventTime.count()<<"us"<<std::endl;
  std::cout <<"number events: "<<ievt.load() -nLanes<<std::endl;
  std::cout <<"----------"<<std::endl;

  source->printSummary();
  out->printSummary();

  return 0;
}
