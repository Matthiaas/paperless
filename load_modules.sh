#!/usr/bin/env bash

# You need to run this script in the same subshell to not loose
# the loaded modules:
#    . load_modules.sh

# Use new software stack.
# env2lmod is an alias set somewhere, but I couldn't run it from inside of
# this script, so I used contents of "which env2lmod"  
. /cluster/apps/local/env2lmod.sh

# Paperless dependencies
module load gcc/8.2.0 cmake/3.16.5 openmpi/4.0.2

# mtbl dependencies
module load libtool/2.4.6 lz4/1.8.1.2 snappy/1.1.7 zstd/1.3.0

