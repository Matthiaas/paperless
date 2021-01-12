#!/usr/bin/env bash

ROOT_DIR=$(pwd)
PAP_DIR=papyrus
PAP_DIR_OPENMPI3=${PAP_DIR}_openmpi3.0.1

rm -rf $PAP_DIR $PAP_DIR_OPENMPI3
git clone https://github.com/sobkulir/papyrus

CMAKE_FLAGS=( -DCMAKE_INSTALL_PREFIX=../install -DCMAKE_BUILD_TYPE=Release -DMPIEXEC_NUMPROC_FLAG="-n" -DPAPYRUS_USE_FORTRAN=OFF )

# If on cluster, also build openmpi3.0.1
if [ -d "/cluster" ]
  then
    . ./load_modules.sh
    echo "Building with openmpi3.0.1"
    MPI_CXX_COMPILER_CUSTOM="/cluster/apps/gcc-8.2.0/openmpi-3.0.1-s2niw66n4q2bopate22cvkevcdfief4d/bin/mpicxx"
    MPI_CXX_LIBRARIES_CUSTOM="/cluster/apps/gcc-8.2.0/openmpi-3.0.1-s2niw66n4q2bopate22cvkevcdfief4d/lib/libmpi_cxx.so;/cluster/apps/gcc-8.2.0/openmpi-3.0.1-s2niw66n4q2bopate22cvkevcdfief4d/lib/libmpi.so"
    cp -r papyrus $PAP_DIR_OPENMPI3
    cd $PAP_DIR_OPENMPI3
    rm -rf build install && mkdir -p build && cd build
    cmake .. "${CMAKE_FLAGS[@]}" -DMPI_CXX_COMPILER_CUSTOM="$MPI_CXX_COMPILER_CUSTOM" -DMPI_CXX_LIBRARIES_CUSTOM="$MPI_CXX_LIBRARIES_CUSTOM"
    make -j install
    cd $ROOT_DIR
fi

cd $PAP_DIR
rm -rf build install && mkdir -p build && cd build

cmake .. "${CMAKE_FLAGS[@]}"
make -j install
