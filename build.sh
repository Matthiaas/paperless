#!/usr/bin/env bash
# Usage: $ ./build.sh <target>
#   e.g. $ ./build.sh tests
rm -r build
mkdir build
cd build && cmake .. && make -j4 $1
