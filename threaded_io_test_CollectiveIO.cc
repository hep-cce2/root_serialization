#include "TROOT.h"
#include "TVirtualStreamerInfo.h"
#include "TObject.h"
#include <iostream>
#include <vector>
#include <string>
#include <atomic>
#include <iomanip>
#include <cmath>

#include "outputerFactoryGenerator.h"
#include "sourceFactoryGenerator.h"

#include "Lane.h"
#include "mpi.h"
#include "tbb/task_group.h"
#include "tbb/global_control.h"
#include "tbb/task_arena.h"

namespace {
  std::pair<std::string, std::string> parseCompound(const char* iArg) {
    std::string sArg(iArg);
    auto found = sArg.find('=');
    auto next = found;
    if(found != std::string::npos) {
      return std::pair(sArg.substr(0,found), sArg.substr(found+1));
    }
    return std::pair(sArg, std::string());
  }

  bool checkForIMT(const char* iArg) {
    std::string sArg(iArg);
    auto found = sArg.find('/');
    if(found != std::string::npos) {
      if(sArg.substr(found+1) != "useIMT") {
	std::cout <<"unknown option for threads "<<sArg.substr(found+1)<<std::endl;
	abort();
      }
      return true;
    }
    return false;
  }
}

long firstEvent( int rank, unsigned long long nEvents){
	return rank*nEvents;

}
int main(int argc, char* argv[]) {
 //Initialization of MPI 
 auto ierr = MPI_Init(&argc,&argv);
  if(ierr!=0){
   std::cout<<"Fatal Error in MPI Initializations "<<std::endl;
   exit(1);
  }
  using namespace cce::tf;

  if(not (argc > 1 and argc < 8) ) {
    std::cout <<"1 to 6 arguments required\n"
                "threaded_io_test <Source configuration> [# threads[/useIMT]] [# conconcurrent events] [wait time scale factor] [max # events] [<Outputer configuration>]\n";
    return 1;
  }

  int parallelism = tbb::this_task_arena::max_concurrency();
  bool useIMT=false;
  if(argc > 2) {
    useIMT = checkForIMT(argv[2]);
    parallelism = atoi(argv[2]);
  }

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
  unsigned int nLanes = 4;
  if(argc > 3) {
    nLanes = atoi(argv[3]);
  }

  double scale = 0.;
  if(argc > 4) {
    scale = atof(argv[4]);
  }

  unsigned long long nEvents = std::numeric_limits<unsigned long long>::max();
  if(argc > 5) {
    nEvents = atoi(argv[5]);
  }
  int rank,size;
  MPI_Comm_size(MPI_COMM_WORLD,&size);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);

  unsigned long long nevents = nEvents;
  unsigned long long remainder = 0;
  unsigned int n_per_rank = nEvents/size;
  if(size>1){
    remainder = nEvents%size;
    if(rank==size-1)nevents = n_per_rank+remainder;
    nevents = n_per_rank;
  }
  std::cout<<nEvents<<" "<<size<<" "<<remainder<< " REM"<<std::endl; 
  std::function<std::unique_ptr<OutputerBase>(unsigned int)> outFactory;
  std::string outputerName = "DummyOutputer";
  if(argc == 7) {
    outputerName = argv[6];
    auto [outputType, outputInfo] = parseCompound(argv[6]);
    outFactory = outputerFactoryGenerator(outputType, outputInfo);
    if(not outFactory) {
      std::cout <<"unknown output type "<<outputType<<std::endl;
      return 1;
    }
  } else {
    outFactory = outputerFactoryGenerator(outputerName, "");
  }

  auto [sourceType, sourceOptions] = parseCompound(argv[1]);
  auto sourceFactory = sourceFactoryGenerator(sourceType, sourceOptions);
  if(not sourceFactory) {
    std::cout <<"unknown source type "<<sourceType<<std::endl;
    return 1;
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
    Lane lane(0, source.get(), 0);
    out->setupForLane(0, lane.dataProducts());
    auto pOut = out.get();
    std::cout <<"begin warmup"<<std::endl;
   
    arena.execute([&lane,pOut]() {
        tbb::task_group group;
	std::atomic<long> ievt = 0;
	std::atomic<unsigned int> count{0};
        group.run([&]() {
            lane.processEventsAsync(ievt, group, *pOut, AtomicRefCounter(count));
          });
        group.wait();
      });
  }
  std::cout <<"finished warmup"<<std::endl;
  
  auto out = outFactory(nLanes);
  auto source = sourceFactory(nLanes, nevents);
  lanes.reserve(nLanes);
  for(unsigned int i = 0; i< nLanes; ++i) {
    lanes.emplace_back(i, source.get(), scale);
    out->setupForLane(i, lanes.back().dataProducts());
  }

  std::atomic<long> ievt = {firstEvent(rank,nevents)};//{0};
  std::cout<<"Processing "<<ievt<<" "<<rank<<" Events Per Rank "<<nevents<<std::endl; 
  tbb::task_arena arena(parallelism);

  decltype(std::chrono::high_resolution_clock::now()) start;
  auto pOut = out.get();
  arena.execute([&lanes, &ievt, pOut, &start]() {
    std::atomic<unsigned int> nLanesWaiting{ 0 };
    std::vector<tbb::task_group> groups(lanes.size());
    start = std::chrono::high_resolution_clock::now();
    auto itGroup = groups.begin();
    {
      AtomicRefCounter laneCounter(nLanesWaiting);
      for(auto& lane: lanes) {
        auto& group = *itGroup;
        group.run([&]() {lane.processEventsAsync(ievt,group, *pOut,laneCounter);});
        ++itGroup;
      }
    }
    do {
      for(auto& group: groups) {
	group.wait();
      }
    } while(nLanesWaiting != 0);
    //be sure all groups have fully finished
    for(auto& group: groups) {
      group.wait();
    }
  });

  std::chrono::microseconds eventTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-start);

  //NOTE: each lane will go 1 beyond the # events so ievt is more then the # events
  std::cout <<"----------"<<std::endl;
  std::cout <<"Source "<<argv[1]<<"\n"
            <<"Outputer "<<outputerName<<"\n"
	    <<"# threads "<<parallelism<<"\n"
	    <<"# concurrent events "<<nLanes <<"\n"
	    <<"time scale "<<scale<<"\n"
	    <<"use ROOT IMT "<< (useIMT? "true\n":"false\n");
  std::cout <<"Event processing time: "<<eventTime.count()<<"us"<<std::endl;
  std::cout <<"number events: "<<ievt.load() -nLanes<<std::endl;
  std::cout <<"----------"<<std::endl;

  source->printSummary();
  out->printSummary();
}
