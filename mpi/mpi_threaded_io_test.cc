#include "TROOT.h"
#include "TVirtualStreamerInfo.h"
#include "TObject.h"
#include <iostream>
#include <iterator>
#include <fstream>
#include <vector>
#include <string>
#include <atomic>
#include <iomanip>
#include <cmath>

#include "../outputerFactoryGenerator.h"
#include "../sourceFactoryGenerator.h"

#include "../Lane.h"

#include "tbb/task_group.h"
#include "tbb/global_control.h"
#include "tbb/task_arena.h"

#include "CLI11.hpp"
#include "mpicpp.h"


namespace {
  std::vector<std::string> fnames(std::string flist) {
    std::ifstream is(flist);
    std::istream_iterator<std::string> i_start(is), i_end;
    return std::vector<std::string>(i_start, i_end);
  }   

  
  std::pair<std::string, std::string> iofiles(int mode, int rank, std::vector<std::string>ilist, std::vector<std::string> olist) {
    if (mode == 0) return {ilist[rank], olist[rank]};
    if (mode == 1) return {ilist[rank], olist[0]};
    if (mode == 2) return {ilist[0], olist[0]};
    return {ilist[0], olist[rank]};
  }
  
  long firstEvent(int mode, int my_rank, unsigned long long nEvents) {
    if (mode == 0 || mode == 1) return 0;
    return my_rank*nEvents;
  }
}

int main(int argc, char* argv[]) {
  Mpi mpi(argc, argv); 
  auto const my_rank =  mpi.rank();
  auto const nranks = mpi.np();
  using namespace cce::tf;

  CLI::App app{"Mpi_threaded_IO"};
  int mode; 
  // assuming n ranks, here are different mode values and their meanings
  // 0: n-n, i.e. n ranks reading n files and writing n files (1 input/output file per rank), should work for all output types
  // 1: n-1, i.e. n ranks reading n files and writing 1 file (1 input file per rank, 1 output file for all ranks), valid only for writig HDF5 output
  // 2: 1-1, valid for HDF5 output only in case of n ranks, and for all output formats if there is only 1 rank. 
  // 3: 1-n , should work for all output types
  std::string isource;
  std::string ilist;
  int jt;
  bool useIMT;
  int je;
  double scale;
  int nevents;
  std::string outputType;
  std::string olist;

  app.add_option("-m, --mode", mode, "Operation mode")->required();
  app.add_option("-s, --source", isource, "Input Source")->required();
  app.add_option("--ilist", ilist, "Name of file with a listing of input file names to be read")->required();
  app.add_option("--jt", jt, "Number of threads")->required();
  app.add_option("--useIMT", useIMT, "Number of threads")->required();
  app.add_option("--je", je, "Number of events per thread")->required();
  app.add_option("--scale", scale, "Scale")->required();
  app.add_option("-n, --nevents", nevents, "Total number of events to process")->required();
  app.add_option("-o, --outputer", outputType, "Output Source")->required();
  app.add_option("--olist", olist, "Name of file with a listing of output file names to be generated")->required();
  app.parse(argc, argv);
  
  int parallelism = tbb::this_task_arena::max_concurrency();
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
  unsigned int nLanes = je;

  unsigned long long nEvents = nevents; //std::numeric_limits<unsigned long long>::max();

  std::vector<std::string> outputInfo = fnames(olist);
  std::vector<std::string> sourceOptions = fnames(ilist);
  auto [ifile, ofile] = iofiles(mode, my_rank, sourceOptions, outputInfo);
  if (outputType == "TextDumpOutputer") ofile = "";
  auto outFactory = outputerFactoryGenerator(outputType,ofile); 
  auto sourceFactory = sourceFactoryGenerator(isource, ifile);

  if (not sourceFactory) {
    std::cout <<"unknown source type "<<isource<<std::endl;
    return 1;
  }
 
 // checking operation modes to make sure we only run valid combos
 //
  switch (mode) {
  case 0:
   if (sourceOptions.size() != outputInfo.size() ) {
    std::cout << "Number of Input and output files dont match\n";
    return 1;
  } else {
    if (sourceOptions.size() != nranks) {
      std::cout << "Number of ranks dont match input/output files\n";
      return 1;
    }
  }
  break;
  case 1:
    //only supported for HDFOutputer
    if (!outputType.compare("HDFOutputer")) {
      std::cout << "Writing to a single file is only supported for HDF5\n";
      return 1;
    }
    break;
  case 2:
    //only supported for HDFOutputer
    if (!outputType.compare("HDFOutputer")) {
      std::cout << "Writing to a single file is only supported for HDF5\n";
      return 1;
    }
    break;
  default:
  break;
}

  mpi.barrier();
  std::cout <<"begin warmup"<<std::endl;
  {
    //warm up the system by processing 1 event 
    tbb::task_arena arena(1);
    auto out = outFactory(1);
    auto source =sourceFactory(1,1);
    Lane lane(0, source.get(), 0);
    out->setupForLane(0, lane.dataProducts());
    auto pOut = out.get();
    arena.execute([&lane,pOut]() {
        tbb::task_group group;
        std::atomic<long> ievt{0};
	std::atomic<unsigned int> count{0};
        group.run([&]() {
            lane.processEventsAsync(ievt, group, *pOut, AtomicRefCounter(count));
          });
        group.wait();
      });
  }
  std::cout <<"finished warmup"<<std::endl;

  mpi.barrier();
  auto out = outFactory(nLanes);
  auto source = sourceFactory(nLanes, nEvents*(my_rank+1));
  lanes.reserve(nLanes);
  for(unsigned int i = 0; i< nLanes; ++i) {
    lanes.emplace_back(i, source.get(), scale);
    out->setupForLane(i, lanes.back().dataProducts());
  }

  std::atomic<long> ievt = firstEvent(mode, my_rank, nEvents);
  
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
  mpi.barrier();
  std::chrono::microseconds eventTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-start);

  //NOTE: each lane will go 1 beyond the # events so ievt is more then the # events
  std::cout <<"----------"<<std::endl;
  std::cout <<"Source "<<argv[1]<<"\n"
            <<"Outputer "<<ofile<<"\n"
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
