#!/usr/bin/env bash
# Usage: $ ./build.sh <target>
#   e.g. $ ./build.sh tests

# If on Euler, set up modules.
if [ -d "/cluster" ]
  then
    # Use new software stack.
    # env2lmod is an alias set somewhere, but I couldn't run it from inside of
    # this script, so I used contents of "which env2lmod"  
    . /cluster/apps/local/env2lmod.sh
    module load cmake gcc openmpi
fi

rm -r build
mkdir build
cd build && cmake .. && make -j4 $1
