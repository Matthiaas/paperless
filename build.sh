#!/usr/bin/env bash
# Usage: $ ./build.sh <target> <Debug|Release> <cmake flags>
#   e.g. $ ./build.sh tests
#        $ ./build.sh paperless_swig Release -DJAVA_WRAPPERS=ON

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

# Clone and compile papyrus
if [ ! -d "papyrus" ]
  then
    ./install_papyrus.sh
fi

DEFAULT_BUILD_TYPE=Release
BUILD_TYPE="${2:-$DEFAULT_BUILD_TYPE}"

echo ${BUILD_TYPE}
#rm -r build
#mkdir build
cd build && cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DIS_CLUSTER=${IS_CLUSTER} ${@:3} .. && make -j16 $1
