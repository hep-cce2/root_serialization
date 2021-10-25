# adding for mpi version of threaded_io_test
add_executable(mpi_threaded_io_test
  ../DeserializeStrategy.cc
  ../HDFOutputer.cc
  ../HDFSource.cc
  ../Lane.cc
  ../PDSOutputer.cc
  ../PDSSource.cc
  ../RepeatingRootSource.cc
  ../RootOutputer.cc
  ../RootSource.cc
  ../SerialRootSource.cc
  ../SerialTaskQueue.cc
  ../SerializeStrategy.cc
  ../SharedPDSSource.cc
  ../TBufferMergerRootOutputer.cc
  ../TestProductsOutputer.cc
  ../TestProductsSource.cc
  ../TextDumpOutputer.cc
  ../UnrolledDeserializer.cc
  ../UnrolledSerializer.cc
  ../common_unrolling.cc
  ../outputerFactoryGenerator.cc
  ../pds_reading.cc
  ../sourceFactoryGenerator.cc
  mpi_threaded_io_test.cc)

target_include_directories(mpi_threaded_io_test PRIVATE "${PROJECT_BINARY_DIR}" ${HDF5_DIR}/include ${MPI_DIR}/include ${CLI11_DIR})
target_link_directories(mpi_threaded_io_test PRIVATE ${HDF5_DIR}/lib ${MPI_DIR}/lib)

target_link_libraries(mpi_threaded_io_test
                      PRIVATE LZ4::lz4
                              ROOT::Core
                              ROOT::RIO
                              ROOT::Tree
                              TBB::tbb
                              Threads::Threads
                              configKeys
                              sequence_classes_dict
                              zstd::libzstd_shared
                              hdf5
                              ${MPI_DIR}/lib/libmpi.so)
