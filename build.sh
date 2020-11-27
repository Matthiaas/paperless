#!/usr/bin/env bash
# Usage: $ ./build.sh <target>
#   e.g. $ ./build.sh tests

# Check if we're on a cluster.
[ -d "/cluster" ]
if [ $? -eq 0 ]
  then
    IS_CLUSTER=ON
else
  IS_CLUSTER=OFF
fi

# Set up modules.
if [ $IS_CLUSTER == "ON" ]
  then
    # Use new software stack.
    # env2lmod is an alias set somewhere, but I couldn't run it from inside of
    # this script, so I used contents of "which env2lmod"  
    . /cluster/apps/local/env2lmod.sh
    module load cmake/3.16.5 gcc/8.2.0 openmpi/4.0.2
fi

rm -r build
mkdir build
cd build && cmake -DIS_CLUSTER=${IS_CLUSTER} .. && make -j4 $1
