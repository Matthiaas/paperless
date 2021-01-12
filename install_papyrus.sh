#!/usr/bin/env bash

# Clones and compiles papyrus
if [ -d "/cluster" ]
  then
    . ./load_modules.sh
    echo "On Euler by default Papyrus is built with gcc-8.2 and OpenMPI 4."
    MPIEXEC="/cluster/apps/gcc-8.2.0/openmpi-4.0.2-vvr7fdofwljfy4qgkkhao36r5qx44hni/bin/mpicc"
else
  MPIEXEC=$(command -v mpicc)
fi

rm -rf papyrus
git clone https://code.ornl.gov/eck/papyrus.git
cd papyrus

rm -rf build install
mkdir -p build
cd build

cmake .. -DCMAKE_INSTALL_PREFIX=../install -DCMAKE_BUILD_TYPE=Release -DMPIEXEC="$MPIEXEC" -DMPIEXEC_NUMPROC_FLAG="-n" -DPAPYRUS_USE_FORTRAN=OFF #Euler
make -j install
