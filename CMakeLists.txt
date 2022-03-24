# ============================================================================
# Configuration instructions
#
# cmake <dir. with this file> \
#   -DROOT_DIR=$ROOTSYS/cmake \
#   -DHDF5_DIR=<HDF5 dir> \
#   -Dzstd_DIR=<path to zstd lib>/cmake/zstd

cmake_minimum_required(VERSION 3.15)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# set the project name
project(threaded_io_test VERSION 0.1 LANGUAGES CXX)

list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake-modules)

# want debug symbols

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g3")

find_package(ROOT REQUIRED COMPONENTS Core RIO Tree)
find_package(TBB REQUIRED)
find_package(zstd REQUIRED)
find_package(lz4 REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

configure_file(ThreadedIOTestConfig.h.in ThreadedIOTestConfig.h)

#make the library for testing
add_library(configKeys configKeyValuePairs.cc)
add_library(configParams ConfigurationParameters.cc)

#make the library holding the root dictionaries
REFLEX_GENERATE_DICTIONARY(G__sequence_classes SequenceFinderForBuiltins.h SELECTION classes_def.xml)

add_library(sequence_classes_dict SHARED G__sequence_classes.cxx)
target_link_libraries(sequence_classes_dict PUBLIC ROOT::RIO ROOT::Net)


#make the library holding the root dictionaries for RootBatchEvents
REFLEX_GENERATE_DICTIONARY(G__batchevents_classes EventIdentifiers.h SELECTION batchevents_def.xml)

add_library(batchevents_classes_dict SHARED G__batchevents_classes.cxx)
target_link_libraries(batchevents_classes_dict PUBLIC ROOT::RIO ROOT::Net)

add_executable(threaded_io_test
  DeserializeStrategy.cc
  EmptySource.cc
  DummyOutputer.cc
  SerializeOutputer.cc
  Lane.cc
  PDSOutputer.cc
  PDSSource.cc
  RepeatingRootSource.cc
  RootOutputerConfig.cc
  RootOutputer.cc
  RootSource.cc
  SerialRootSource.cc
  RootEventOutputer.cc
  SharedRootEventSource.cc
  RootBatchEventsOutputer.cc
  SharedRootBatchEventsSource.cc
  SerialTaskQueue.cc
  SerializeStrategy.cc
  SharedPDSSource.cc
  TBufferMergerRootOutputer.cc
  TestProductsOutputer.cc
  TestProductsSource.cc
  TextDumpOutputer.cc
  UnrolledDeserializer.cc
  UnrolledSerializer.cc
  common_unrolling.cc
  ConfigurationParameters.cc
  OutputerFactory.cc
  outputerFactoryGenerator.cc
  WaiterFactory.cc
  waiterFactoryGenerator.cc
  ScaleWaiter.cc
  EventSleepWaiter.cc
  EventUnevenSleepWaiter.cc
  pds_reading.cc
  pds_writer.cc
  pds_common.cc
  SourceFactory.cc
  sourceFactoryGenerator.cc
  threaded_io_test.cc)

target_link_libraries(threaded_io_test
                      PRIVATE LZ4::lz4
                              ROOT::Core
                              ROOT::RIO
                              ROOT::Tree
                              TBB::tbb
                              Threads::Threads
                              configKeys
                              sequence_classes_dict
                              batchevents_classes_dict
                              zstd::libzstd_shared)

add_subdirectory(cms)
add_subdirectory(test_classes)

add_executable(unroll_test 
  UnrolledDeserializer.cc 
  UnrolledSerializer.cc
  common_unrolling.cc
  unroll_test.cc)

target_link_libraries(unroll_test
                      PRIVATE ROOT::Core
                              ROOT::RIO
                              ROOT::Tree
                              TBB::tbb
                              cms_dict
                              sequence_classes_dict
                              test_classes_dict)

enable_testing()
add_subdirectory(tests)
add_test(NAME EmptySourceTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10)
add_test(NAME DummyOutputerTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o DummyOutputer)
add_test(NAME DummyOutputerUseProductReadyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o DummyOutputer=useProductReady)
add_test(NAME TextDumpOutputerPerEventTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o TextDumpOutputer=perEvent=t)
add_test(NAME TextDumpOutputerSummaryTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o TextDumpOutputer=summary=t)
add_test(NAME TestProductsTest COMMAND threaded_io_test -s TestProductsSource -t 1 -n 10 -o TestProductsOutputer)
add_test(NAME SerializeOutputerTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o SerializeOutputer)
add_test(NAME SerializeOutputerVerboseTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o SerializeOutputer=verbose)
add_test(NAME PDSOutputerEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o PDSOutputer=test_empty.pds)
add_test(NAME TestProductsPDS COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o PDSOutputer=test_prod.pds; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s SharedPDSSource=test_prod.pds -t 1 -n 10 -o TestProductsOutputer")
add_test(NAME PDSOutputerAllOptionsEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o PDSOutputer=test_empty.pds:compressionLevel=8:compressionAlgorithm=LZ4:serializationAlgorithm=Unrolled)
add_test(NAME TestProductsPDSUnrolled COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o PDSOutputer=test_prod_unroll.pds:serializationAlgorithm=Unrolled; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s SharedPDSSource=test_prod_unroll.pds -t 1 -n 10 -o TestProductsOutputer")
add_test(NAME TestProductsPDSUncompressed COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o PDSOutputer=test_prod.pds:compressionAlgorithm=None; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s SharedPDSSource=test_prod.pds -t 1 -n 10 -o TestProductsOutputer")
add_test(NAME RootOutputerEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o RootOutputer=test_empty.root)
add_test(NAME RootOutputerEmptySplitLevelTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o RootOutputer=test_empty.root:splitLevel=1)
add_test(NAME RootOutputerEmptyAllOptionsTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o RootOutputer=test_empty.root:splitLevel=1:compressionLevel=1:compressionAlgorithm=LZMA:basketSize=32000:treeMaxVirtualSize=-1:autoFlush=900)
add_test(NAME TestProductsROOT COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o RootOutputer=test_prod.root; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s SerialRootSource=test_prod.root -t 1 -n 10 -o TestProductsOutputer")
add_test(NAME TestProductsROOTReplicated COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o RootOutputer=test_prod_repl.root; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s ReplicatedRootSource=test_prod_repl.root -t 1 -n 10 -o TestProductsOutputer")
add_test(NAME TestProductsROOTRepeating COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o RootOutputer=test_prod_rep.root; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s RepeatingRootSource=test_prod_rep.root:repeat=5 -t 1 -n 100 -o TestProductsOutputer")
add_test(NAME TestProductsROOTRepeatingOneBranch COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o RootOutputer=test_prod_rep_1branch.root; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s RepeatingRootSource=test_prod_rep_1branch.root:repeat=5:branchToRead=floats -t 1 -n 100 -o TestProductsOutputer")

add_test(NAME RootEventOutputerEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o RootEventOutputer=test_empty.eroot)
add_test(NAME TestProductsRootEvent COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o RootEventOutputer=test_prod.eroot; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s SharedRootEventSource=test_prod.eroot -t 1 -n 10 -o TestProductsOutputer")
add_test(NAME RootEventOutputerAllOptionsEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o RootEventOutputer=test_empty.eroot:compressionLevel=8:compressionAlgorithm=LZ4:serializationAlgorithm=Unrolled)
add_test(NAME TestProductsRootEventUnrolled COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o RootEventOutputer=test_prod_unroll.eroot:serializationAlgorithm=Unrolled; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s SharedRootEventSource=test_prod_unroll.eroot -t 1 -n 10 -o TestProductsOutputer")

add_test(NAME RootBatchEventsOutputerEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o RootBatchEventsOutputer=test_empty.broot)
add_test(NAME TestProductsRootBatchEvents COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o RootBatchEventsOutputer=test_prod.broot; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s SharedRootBatchEventsSource=test_prod.broot -t 1 -n 10 -o TestProductsOutputer")
add_test(NAME TestProductsRootBatchEventsBatchSize COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o RootBatchEventsOutputer=test_prod.broot:batchSize=4; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s SharedRootBatchEventsSource=test_prod.broot -t 1 -n 10 -o TestProductsOutputer")

add_test(NAME TBufferMergerRootOutputerEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o TBufferMergerRootOutputer=test_empty.root)
add_test(NAME TBufferMergerRootOutputerEmptySplitLevelTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o TBufferMergerRootOutputer=test_empty.root:splitLevel=1)
add_test(NAME TBufferMergerRootOutputerEmptyAllOptionsTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o TBufferMergerRootOutputer=test_empty.root:splitLevel=1:compressionLevel=1:compressionAlgorithm=LZMA:basketSize=32000:treeMaxVirtualSize=-1:autoFlush=900)
add_test(NAME UseIMTTest COMMAND threaded_io_test -s EmptySource -t 1 --use-IMT=t -n 10)
add_test(NAME ScaleWaiterTest COMMAND threaded_io_test -s TestProductsSource -t 1 -n 10 -w ScaleWaiter=scale=1000.)
add_test(NAME EventSleepWaiterTest COMMAND bash -c "echo 300000 > times.wait; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -w EventSleepWaiter=filename=times.wait")

option(ENABLE_HDF5 "Build HDF5 Sources and Outputers" ON) # default ON
if(ENABLE_HDF5)
  if(NOT DEFINED HDF5_DIR)
    message(FATAL_ERROR "You must provide HDF5_DIR variable")
  endif()
  target_sources(threaded_io_test PRIVATE
    multidataset_plugin.cc
    H5Timing.cc
    HDFEventOutputer.cc
    HDFBatchEventsOutputer.cc
    HDFOutputer.cc
    HDFSource.cc)
  target_include_directories(threaded_io_test PRIVATE "${PROJECT_BINARY_DIR}" ${HDF5_DIR}/include)
  target_link_directories(threaded_io_test PRIVATE ${HDF5_DIR}/lib)
  target_link_libraries(threaded_io_test PRIVATE hdf5 hdf5_hl)
  add_test(NAME HDFOutputerEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o HDFOutputer=test_empty.h5)
  add_test(NAME TestProductsHDF COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o HDFOutputer=test_prod.h5; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s HDFSource=test_prod.h5 -t 1 -n 10 -o TestProductsOutputer")
  add_test(NAME HDFEventOutputerEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o HDFEventOutputer=test_empty_event.h5)
  add_test(NAME HDFBatchEventsOutputerEmptyTest COMMAND threaded_io_test -s EmptySource -t 1 -n 10 -o HDFBatchEventsOutputer=test_empty_event.h5)
  add_test(NAME TestProductsHDFEvent COMMAND bash -c "${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s TestProductsSource -t 1 -n 10 -o HDFEventOutputer=test_prod_e.h5")
  #; ${CMAKE_CURRENT_BINARY_DIR}/threaded_io_test -s HDFSource=test_prodi_e.h5 -t 1 -n 10 -o TestProductsOutputer")
endif()

option(ENABLE_S3 "Build S3 Sources and Outputers" OFF) # default OFF
if(ENABLE_S3)
  if(NOT DEFINED LIBS3_DIR)
    message(FATAL_ERROR "You must provide LIBS3_DIR variable")
  endif()
  target_sources(threaded_io_test PRIVATE
    S3Outputer.cc
    S3Common.cc
    )
  target_include_directories(threaded_io_test PRIVATE ${LIBS3_DIR}/include)
  target_link_directories(threaded_io_test PRIVATE ${LIBS3_DIR}/lib)
  target_link_libraries(threaded_io_test PRIVATE s3)
  # add_test(NAME S3OutputerEmptyTest COMMAND threaded_io_test EmptySource 1 1 0 10 S3Outputer)
endif()
