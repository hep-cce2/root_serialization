# S3 I/O components

As part of the "Object Storage for CMS in the HL-LHC era" project, the
`S3Source` and `S3Outputer` provide an IO system that can write to S3 buckets,
with the main purpose to explore the performance and parallelization
capabilities of the Ceph RadosGW S3 service.

## Building
To build with S3 support, you will need to download and install [libs3](https://github.com/bji/libs3):
```bash
mkdir -p external
git clone git@github.com:bji/libs3.git
cd libs3
make DESTDIR=../external install
```

the rest of the dependencies can be sourced, e.g., from a recent CMSSW release:
```bash
source /cvmfs/cms.cern.ch/slc7_amd64_gcc10/external/cmake/3.18.2/etc/profile.d/init.sh
pushd /cvmfs/cms.cern.ch/slc7_amd64_gcc10/cms/cmssw/CMSSW_12_3_0_pre5/
cmsenv
popd
git clone git@github.com:hep-cce2/root_serialization.git
cd root_serialization
mkdir build && cd build
cmake ../ \
  -DCMAKE_PREFIX_PATH="/cvmfs/cms.cern.ch/slc7_amd64_gcc10/external/lz4/1.9.2-373b1f6c80ba13e93f436c77aa63c026;/cvmfs/cms.cern.ch/slc7_amd64_gcc10/external/protobuf/3.15.1-b2ca6d3fa59916150b27c3d598c7c7ac;/cvmfs/cms.cern.ch/slc7_amd64_gcc10/external/xz/5.2.5-d6fed2038c4e8d6e04531d1adba59f37" \
  -Dzstd_DIR=/cvmfs/cms.cern.ch/slc7_amd64_gcc10/external/zstd/1.4.5-ec760e16a89e932fdc84f1fd3192f206/lib/cmake/zstd \
  -DTBB_DIR=/cvmfs/cms.cern.ch/slc7_amd64_gcc10/external/tbb/v2021.4.0-75e6d730601d8461f20893321f4f7660/lib/cmake/TBB \
  -DROOT_DIR=$ROOTSYS/cmake \
  -DLIBS3_DIR=$(realpath ../../external) \
  -DENABLE_HDF5=OFF -DENABLE_S3=ON
```

## Running with local server
The S3 connection settings are specified in an ini file. A local server can be
brought up using the `./s3localserver.sh` script, assuming singularity is
available at your site. Then use `conn=s3conn_local.ini` in the source/outputer
configuration.

## Data layout
There are two types of binary data blobs written to the S3 service:
- An event index, unique per processing task, is stored at `index/{prefix}`. It contains an event number index, stored as a list of compressed _event stripes_, and is rewritten after each new event stripe is flushed. The frequency of this is controlled by the `eventFlush` parameter in S3Outputer.
- Several product _stripes_, stored at `{prefix}/{product_name}/{offset}` where the offset indexes the event stripe list. The number of product stripes written per event stripe depends on the size of the product and the `productFlush` parameter, as well as the requirement that the number of events worth of data products per stripe is divisible by `eventFlush`.

The binary data format of the blobs is specified by the `objectstripe.proto` protobuf schema.
