# root_serialization

This is a testing structure for doing performance experiments with various I/O packages from within an multi-threaded program. The program mimics behaviors common for
HEP data processing frameworks.

## Getting started
The CMakeLists.txt uses 3rd party packages from /cvmfs which are identical to the packages used by the CMS experiment's CMSSW_11_2_0_pre6 software release. No CMS
code is needed to build the code from this repository. CMS header files and shared libraries are needed if data files containing CMS data are being read.

To build

```
$ git clone <root_serialization URL>
$ mkdir build-root-serialization
$ cd build-root-serialization
$ cmake <path to root_serialization> \
  -DROOT_DIR=path_to_ROOT_cmake_targets \
  [-DTBB_DIR=path_to_tbb_cmake_targets] \
  [-DZSTD_DIR=path_to_zstd_cmake_targets] \
  [-DLZ4_DIR=path_to_lz4_cmake_targets]
$ make [-j N]
```

This will create the executable `threaded_io_test`.

If no cmake target exists for your installation of lz4, you can replace the cmake command above with

```
$ cmake <path to root_serialization> \
  -DCMAKE_PREFIX_PATH=path_to_lz4_install \
  -DROOT_DIR=path_to_ROOT_cmake_targets \
  [-DTBB_DIR=path_to_tbb_cmake_targets] \
  [-DZSTD_DIR=path_to_zstd_cmake_targets] \
  [-DLZ4_DIR=path_to_lz4_install]
$ make [-j N]
```

where `path_to_lz4_install` is the path to the folder holding the `include` and `lib` directories containing the files for lz4.

Note that CMake
can use some of the information from ROOT's CMake configuration to
infer the build information for TBB, zstd, and lz4.  If the ROOT
runtime is setup, one can use `-DROOT_DIR=$ROOTSYS/cmake`.

## threaded_io_test design
The testing structure has 3 customizable component types
1. `Source`s: These supply the _event_ data products used for testing.
1. `Outputer`s: These read the _event_ data products. Some Outputers also write that data out for storage.
1. `Waiter`s: These get called for each _event_ data product after the data products are read from testing. The `Outputer` is called
once the `Waiter` has finished its asynchronous callback for the data product. This allows tuning the timing of events to better mimic actual HEP data processing.

The testing structure can process multiple _events_ concurrently. The processing of 
an _event_ is handled by a `Lane`. Concurrent _events_ are then done by having multiple `Lane`s.

All `Lane`s share the same `Source`, `Waiter`, and `Outputer`. Therefore all three of these types are required to be thread safe.

The way data is processed is as follows:
1. When a `Lane` is no longer processing an _event_ it requests a new one from the system. The system advances an atomic counter and tells the `Lane` to use the _event_ associated with that index.
1. The `Lane` then passes the _event_ index to the `Source` and asks it to asynchronously retrieve the _event_ data products.
1. Once the `Source` has retrieved the data products, it signals to the `Waiter`s to run asynchronously.
1. Once the`Waiter` has finished with each data product, the system signals to the `Outputer` that the particular _event_ data product is available for the `Outputer`. The `Outputer`
can then asynchronously process that data product.
1. Once the `Outputer` has finished with all the data products, the system signals the `Outputer` that the _event_ has finished. The `Outputer` can then do
additional asynchronous work.
1. When the `Outputer` finishes its end of _event_ work, the `Lane` is considered to be done with that _event_ and the cycle repeats.

## Running tests
The `threaded_io_test` takes the following command line arguments
```
threaded_io_test -s <Source configuration> [-t <# threads>] [--use-IMT=<T/F>] [-l <# conconcurrent events>] [-w <Waiter configuration>] [ -n <max # events>] [-o <Outputer configuration>]
```

1. `--source, -s` `<Source configuration>` : which `Source` to use and any additional information needed to configure it. Options are described below.
1. `--num-threads, -t` `<# threads>` : number of threads to use in the job. 
1. `--use-IMT` turn on or off ROOT's implicit multithreaded (IMT). Default is off.
1. `--num-lanes, -l` `<# concurrent events>` : number of concurrent _events_ (that is `Lane`s) to use. Best if number of events is less than  or equal to number of threads. Default is the value used for `--num-threads`.
1. `--waiter, -w` `<Waiter configuration>` : used to specify which `Waiter` to use and any additional information needed to configure it. The exact options are described below. Default is '' which causes no `Waiter` to be used.
1. `--num-events, -n` `<max # events>` : max number of events to process in the job. Default is largest possible 64 bit value.
1. `--outputer, -o`  `<Outputer configuration>` : used to specify which `Outputer` to use and any additional information needed to configure it. The exact options are described below. Default is `DummyOutputer`.

## Available Components

### Sources

#### EmptySource
Does not generate any _event_ data products. Specify by just using its name, e.g. 
```
> threaded_io_test -s EmptySource -t 1 -n 10
```

#### TestProductsSource
Generates a fixed set of data products for C++ types known by ROOT. Specify by just using its name, e.g.
```
> threaded_io_test -s TestProductsSource -t 1 -n 10
```

#### ReplicatedRootSource
Reads a standard ROOT file. Each concurrent Event has its own replica of the Source to avoid the need for cross Event synchronization. In addition to its name, one needs to give the file to read, e.g.
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10
```

#### SerialRootSource
Reads a standard ROOT file. All concurrent Events share the same Source. Access to the Source is serialized for thread-safety. In addition to its name, one needs to give the file to read, e.g.
```
> threaded_io_test -s SerialRootSource=test.root -t 1 -n 1000
```


#### RepeatingRootSource
Reads the first N events from a standard ROOT file at construction time. The deserialized data products are held in memory. Going from event to event is just a switch of the memory addresses to be used. In addition to its name, one needs to give the file to read and, optionally, the number of events to read (default is 10) and a singular TBranch to read, e.g.

```
> threaded_io_test -s RepeatingRootSource=test.root -t 1  -n 1000
```
or
```
> threaded_io_test -s RepeatingRootSource=test.root:repeat=5 -t 1 -n 1000
```
or
```
> threaded_io_test -s RepeatingRootSource=test.root:repeat=5:branchToRead=ints -t 1 -n 1000
```



#### ReplicatedPDSSource
Reads a _packed data streams_ format file. Each concurrent Event has its own replica of the Source to avoid the need for cross Event synchronization. In addition to its name, one needs to give the file to read, e.g.
```
> threaded_io_test -s ReplicatedPDSSource=test.pds -t 1 -n 10
```

#### SharedPDSSource
Reads a _packed data streams_ format file. The Source is shared between the concurrent Events. Reads from the file are serialized for thread-safety while decompressing the Event and the object deserialization can proceed concurrently. In addition to its name, one needs to give the file to read, e.g.
```
> threaded_io_test -s SharedPDSSource=test.pds -t 1 -n 10
```

#### SharedRootEventSource
Reads a ROOT file which only has 2 TBranches in the `Events` TTree. One branch holds the EventIdentifier. The other holds a (possibly pre-compressed) buffer of all the pre-object serialized data products in the event and a vector of offsets into that buffer for the beginning of each data products serialization. The Source is shared between the concurrent Events. Reads from the file are serialized for thread-safety and decompressing the Event happens at that time as well. The object deserialization can proceed concurrently. In addition to its name, one needs to give the file to read, e.g.
```
> threaded_io_test -s SharedRootEventSource=test.eroot -t 1 -n 10
```

#### SharedRootBatchEventsSource
This is similar to SharedRootEventSource except this time each entry in the `Events` TTree is actually for a batch of Events. The `Events` TTree again only holds 2 TBranches. One branch holds a `std::vector<EventIdentifier>`. The other holds a (possibly pre-compressed) buffer of all the pre-object serialized data products for all the events in the batch and a vector of offsets into that buffer for the beginning of each data products serialization. The Source is shared between the concurrent Events. Reads from the file are serialized for thread-safety and decompressing the Event happens at that time as well. The object deserialization can proceed concurrently. In addition to its name, one needs to give the file to read, e.g.
```
> threaded_io_test -s SharedRootBatchEventsSource=test.eroot -t 1 -n 10
```

#### S3Source
Reads individual data product _stripes_--compressed concatenated serialized data products--from an S3 server, along with the appropriate
event-level metadata to index them. See s3io.md for further details of the data layout.

- verbose (int): increase number to get more detail
- prefix (string): the object prefix in the S3 bucket
- conn (string): path to the S3 connection configuration file
```
> threaded_io_test -s S3Source=prefix=testproducts:conn=s3conn.ini -t 1 -n 10
```

### Outputers

#### DummyOutputer
Does no work. If no outputer is given, this is the one used. Specify by just using its name and optionally
the option label 'useProductReady'
```
> threaded_io_test -s EmptySource -t 1 -n 10 -o DummyOutputer
```
or
```
> threaded_io_test -s EmptySource -t 1 -n 10 -o DummyOutputer=useProductReady
```

#### TextDumpOutputer
Dumps the name and sizes for each data product. Specify by its name and the following optional parameters:
- perEvent: print names and sizes for each event. On by default. Allowed values are `perEvent=t`, `perEvent=f` and `perEvent` which is same as `perEvent=t`.
- summary: at end of job, print names and average size per event. Off by default. Allowed values are `summary=t`, `summary=f`, and `summary` which is same as `summary=t`.
```
> threaded_io_test -s EmptySource -t 1 -n 10 -o TextDumpOutputer
```
or
```
> threaded_io_test -s EmptySource -t 1 -n 10 -o TextDumpOutputer=perEvent=f:summary=t
```

#### SerializeOutputer
Uses ROOT to serialize the _event_ data products but does not store them. It prints timing statistics about the serialization. Specify by just using its name and an optional 'verbose' parameter
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o SerializeOutputer
```
or
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o SerializeOutputer=verbose
```

#### TestProductsOutputer
Checks that the data products match what is expected from TestProductsSource or files containing those same data products. If the results are unexpected, the program will abort. Specify by just using its name.
```
> threaded_io_test -s TestProductsSource -t 1 -n 10 -o TestProductsOutputer
```

#### RootOutputer
Writes the _event_ data products into a ROOT file. Specify both the name of the Outputer and the file to write as well as many  optional parameters:
- splitLevel: split level for the branches, default 99
- compressionLevel: compression level 0-9, default 9
- compressionAlgorithm: name of compression algorithm. Allowed valued "", "ZLIB", "LZMA", "LZ4"
- basketSize: default size of all baskets, default size 16384
- treeMaxVirtualSize: Size of ROOT TTree TBasket cache. Use ROOT default if value is <0. Default -1.
- autoFlush: passed value to TTree SetAutoFlush. Use of the default value -1 means no call is made.
- cacheSize: size in bytes passed to TFileCacheWrite. Use of the dafault value 0 means cache is set to 0.
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o RootOutputer=test.root
```
or
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o RootOutputer=test.root:splitLevel=1
```
or
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o RootOutputer=test.root:splitLevel=1:cacheSize:1048576
```

#### TBufferMergerRootOutputer
Writes the _event_ data products into a buffer that is then written to a ROOT file. Specify both the name of the Outputer and the file to write as well as many  optional parameters:
- splitLevel: split level for the branches, default 99
- compressionLevel: compression level 0-9, default 9
- compressionAlgorithm: name of compression algorithm. Allowed valued "", "ZLIB", "LZMA", "LZ4"
- basketSize: default size of all baskets, default size 16384
- treeMaxVirtualSize: Size of ROOT TTree TBasket cache. Use ROOT default if value is <0. Default -1.
- autoFlush: passed value to TTree SetAutoFlush. Use of the default value -1 means no call is made.
- cacheSize: size in bytes passed to TFileCacheWrite. Use of the dafault value 0 means cache is set to 0.
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o TBufferMergerRootOutputer=test.root
```
or
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o TBufferMergerRootOutputer=test.root:splitLevel=1
```
or
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o TBufferMergerRootOutputer=test.root:splitLevel=1:cacheSize:1048576
```


#### PDSOutputer
Writes the _event_ data products into a PDS file. Specify both the name of the Outputer and the file to write as well as compression options:
- compressionLevel: compression level. Allowed value depends on algorithm. For now ZSTD is the only one and allows values
  - 0 - 19 (negative values and values 20-22 are possible but not considered good choices by the zstandard authors)
- compressionAlgorithm: name of compression algorithm. Allowed valued "", "None", "ZSTD", "LZ4"
- serializationAlgorithm: name of a serialization algorithm. Allowed values "", "ROOT", "ROOTUnrolled" or "Unrolled". The default is "ROOT" (which is the same as ""). Both _unrolled_ names correspond to the same algorithm.
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o PDSOutputer=test.pds
```

#### HDFOutputer
Writes the _event_ data products into a HDF file. Specify both the name of the Outputer and the file to write as well as the number of events to _batch_ together when writing::
- batchSize: number of events to batch together before writing out to the file. Default is 2.
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o HDFOutputer=test.hdf
```
or
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o HDFOutputer=test.hdf:batchSize=10
```

#### HDFBatchEventsOutputer
Writes the _event_ data products into a HDF file where all data products for a batch of events are stored in a single dataset where the data products for all the events in the batch have been pre-object serialized into a `std::vector<char>`. Specify both the name of the Outputer and the file to write as well as many  optional parameters:

- hdfchunkSize: HDF chunk size value to use for dataset. Default is 10485760.
- batchSize: number of events to batch together when storing, default 1
- compressionLevel: compression level. Allowed value depends on algorithm. For now ZSTD is the only one and allows values
  - 0 - 19 (negative values and values 20-22 are possible but not considered good choices by the zstandard authors)
- compressionAlgorithm: name of compression algorithm. Allowed values "", "None", "ZSTD", "LZ4"
- compressionChoice: what to compress. Allowed values "None", "Events", "Batch", "Both". Default is "Events".
- serializationAlgorithm: name of a serialization algorithm. Allowed values "", "ROOT", "ROOTUnrolled" or "Unrolled". The default is "ROOT" (which is the same as ""). Both _unrolled_ names correspond to the same algorithm.
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o RootBatchEventsOutputer=test.root
```
or
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o RootBatchEventsOutputer=test.root:batchSize=4
```


#### RootEventOutputer
Writes the _event_ data products into a ROOT file where all data products for an event are stored in a single TBranch where the data products have been pre-object serialized into a `std::vector<char>`. Specify both the name of the Outputer and the file to write as well as many  optional parameters:

- tfileCompressionLevel: compression level to be used by ROOT 0-9, default 0
- tfileCompressionAlgorithm: name of compression algorithm to be used by ROOT. Allowed valued "", "ZLIB", "LZMA", "LZ4"
- treeMaxVirtualSize: Size of ROOT TTree TBasket cache. Use ROOT default if value is <0. Default -1.
- autoFlush: passed value to TTree SetAutoFlush. Use of the default value -1 means no call is made.
- compressionLevel: compression level. Allowed value depends on algorithm. For now ZSTD is the only one and allows values
  - 0 - 19 (negative values and values 20-22 are possible but not considered good choices by the zstandard authors)
- compressionAlgorithm: name of compression algorithm. Allowed valued "", "None", "ZSTD", "LZ4"
- serializationAlgorithm: name of a serialization algorithm. Allowed values "", "ROOT", "ROOTUnrolled" or "Unrolled". The default is "ROOT" (which is the same as ""). Both _unrolled_ names correspond to the same algorithm.
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o RootEventOutputer=test.root
```
or
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o RootEventOutputer=test.root:compressionAlgorithm=LZ4
```

#### RootBatchEventsOutputer
Writes the _event_ data products into a ROOT file where all data products for a batch of events are stored in a single TBranch where the data products for all the events in the batch have been pre-object serialized into a `std::vector<char>`. Specify both the name of the Outputer and the file to write as well as many  optional parameters:

- batchSize: number of events to batch together when storing, default 1
- tfileCompressionLevel: compression level to be used by ROOT 0-9, default 0
- tfileCompressionAlgorithm: name of compression algorithm to be used by ROOT. Allowed valued "", "ZLIB", "LZMA", "LZ4"
- treeMaxVirtualSize: Size of ROOT TTree TBasket cache. Use ROOT default if value is <0. Default -1.
- autoFlush: passed value to TTree SetAutoFlush. Use of the default value -1 means no call is made.
- compressionLevel: compression level. Allowed value depends on algorithm. For now ZSTD is the only one and allows values
  - 0 - 19 (negative values and values 20-22 are possible but not considered good choices by the zstandard authors)
- compressionAlgorithm: name of compression algorithm. Allowed valued "", "None", "ZSTD", "LZ4"
- serializationAlgorithm: name of a serialization algorithm. Allowed values "", "ROOT", "ROOTUnrolled" or "Unrolled". The default is "ROOT" (which is the same as ""). Both _unrolled_ names correspond to the same algorithm.
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o RootBatchEventsOutputer=test.root
```
or
```
> threaded_io_test -s ReplicatedRootSource=test.root -t 1 -n 10 -o RootBatchEventsOutputer=test.root:batchSize=4
```

#### S3Outputer
Outputs individual data product _stripes_--compressed concatenated serialized data products--to an S3 server, along with the appropriate
event-level metadata to index them. See s3io.md for further details of the data layout.

- verbose (int): increase number to get more detail
- prefix (string): the object prefix to use when storing data in the S3 bucket
- productFlush (int): the minimum number of (possibly compressed) bytes to accumulate in the product stripe output buffer before flushing it to S3
- eventFlush (int): the maximum number of events that can be contained in a single product stripe.
- conn (string): path to the S3 connection configuration file
```
> threaded_io_test -s TestProductsSource -t 1 -n 10 -o S3Outputer=prefix=testproducts:conn=s3conn.ini
```

### Waiters

#### ScaleWaiter
For each data product this waiter sleeps for an amount of time proportional to the `size` property of the data product. The configuration options are:
- scale: used to convert the size property of the _event_ data products into microseconds used for a call to sleep. A value of 0 means no sleeping.

#### EventSleepWaiter
This waiter reads a file containing the total time it should sleep for each event. If the number of events in the file is less than the total number of the job, the waiter will repeat the same sleep times. The order of the sleep times is guaranteed to line up with the order of Events coming from the Source. The waiter divides the event sleep time equally among all the data products.
The configuration options are:
- filename: the name of the file containing the event sleep times. The event entries must be separated by white space. The sleep times are in microseconds. 

#### EventUnevenSleepWaiter
Similar to EvenSleep Waiter, this waiter reads a file containing the total time it should sleep for each event. If the number of events in the file is less than the total number of the job, the waiter will repeat the same sleep times. The order of the sleep times is guaranteed to line up with the order of Events coming from the Source. The waiter divides the event sleep time equally among the number of data products specified by the configuration option. This numer must be less than or equal to the number of data products in the job.
The configuration options are:
- filename: the name of the file containing the event sleep times. The event entries must be separated by white space. The sleep times are in microseconds.
- divideBetween: how many tasks that should split the event time equally. Default is the number of data products in the job.
- scale: a floating point value used to multiple with the event times in the file. Default is 1.0.

## unroll_test

The _unroll_test_ executable is meant to allow testing of the unrolled serialization process and allow comparison of object serialization sizes with respect to ROOT's standard serialization. The executable takes the following command line arguments

unroll_test [-g] [-s] [list of class names]

- -g : turns on ROOT verbose debugging output
- -s : skips running the built in test cases
- [list of class names] : names of C++ classes with ROOT dictionaries. The executable will perform serialization/deserialization on defaultly constructed instances of these classes and report the bytes needed for storage.
