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
    . ./load_modules.sh
fi

rm -r build
mkdir build
cd build && cmake -DCMAKE_BUILD_TYPE=Release -DIS_CLUSTER=${IS_CLUSTER} .. && make -j4 $1
