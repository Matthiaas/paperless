#!/usr/bin/env bash

export MAX_LOCAL_MEMTABLE_SIZ=100000
export MAX_REMOTE_MEMTABLE_SIZE=100000
export MAX_LOCAL_CACHE_SIZE=100000
export MAX_REMOTE_CACHE_SIZE=100000
export DISPATCH_IN_CHUNKS=1
export STORAGE_LOCATION=/tmp/PaperlessTest


for i in 1 2 4
do
  mpiexec -n $i ./build/mpi_tests "[${i}rank]"
  for ((j=0; j <$i; j++))
  do
    rm -r /tmp/PaperlessTest${j}
  done
done