#!/usr/bin/env bash
# Usage: $ ./build.sh <target> <Debug|Release>
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
    . ./load_modules.sh
fi

DEFAULT_BUILD_TYPE=Release
BUILD_TYPE="${2:-$DEFAULT_BUILD_TYPE}"

echo ${BUILD_TYPE}
rm -r build
mkdir build
cd build && cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DIS_CLUSTER=${IS_CLUSTER} .. && make -j4 $1
