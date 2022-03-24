```
module load cmake
module load cray-hdf5
module swap PrgEnv-intel PrgEnv-gnu
module swap gcc/11.2.0 gcc/10.3.0

cmake ../root_serialization/ -Dzstd_DIR=/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/zstd-1.5.0-4mdd6ciw3hm5fqbpvipb5vxg3sbipvd4/lib64/cmake/zstd -DROOT_DIR=/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/root-6.26.00-ljiqxun7qst64fagfjqzzsdoh2fb27n2/cmake -DTBB_DIR=/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/intel-tbb-2020.3-pyg4puzoyzd546logbld25e4r3bf2h6h/cmake -DCMAKE_PREFIX_PATH=/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/lz4-1.9.3-gkvb5log7gbikp5bcn4fn5kjchic32g6 -Dnlohmann_json_DIR=/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/nlohmann-json-3.9.1-vomlzbroxofzwy65k66itu4atq4dqxh4/lib64/cmake/nlohmann_json -DCMAKE_CXX_COMPILER=/opt/gcc/10.3.0/bin/gcc -DHDF5_DIR=${HDF5_DIR} -DCMAKE_BUILD_TYPE=RelWithDebInfo

make -j8 

export LD_LIBRARY_PATH=/opt/cray/job/2.2.4-7.0.1.1_3.50__g36b56f4.ari/lib64:/opt/intel/compilers_and_libraries_2019.3.199/linux/compiler/lib/intel64:/opt/intel/compilers_and_libraries_2019.3.199/linux/mkl/lib/intel64:/usr/common/software/darshan/3.2.1/lib:/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/fwdata-master-o55z6pcllili2zskro2iwjqbmmsay454/lib:/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/root-6.26.00-ljiqxun7qst64fagfjqzzsdoh2fb27n2/lib

export ROOT_INCLUDE_PATH=/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/fwdata-master-o55z6pcllili2zskro2iwjqbmmsay454/src:/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/fmt-8.0.1-5bptgwru6v4nse2f2wekkzklzqsrwjac/include:/global/u1/g/gartung/spack/opt/spack/cray-cnl7-haswell/gcc-10.1.0/root-6.26.00-ljiqxun7qst64fagfjqzzsdoh2fb27n2/include
```
