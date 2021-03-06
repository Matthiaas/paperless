#!/usr/bin/env bash

# Have small sizes -> lots of overflows and things happening.
export MAX_LOCAL_MEMTABLE_SIZ=100
export MAX_REMOTE_MEMTABLE_SIZE=100
export MAX_LOCAL_CACHE_SIZE=100
export MAX_REMOTE_CACHE_SIZE=100
export STORAGE_LOCATION=/tmp/PaperlessTest/


export DISPATCH_IN_CHUNKS=0
for i in 1 2 4
do
  mpiexec -n $i ./build/mpi_tests "[${i}rank]"
done

rm -r /tmp/PaperlessTest

export DISPATCH_IN_CHUNKS=1
for i in 1 2 4
do
  mpiexec -n $i ./build/mpi_tests "[${i}rank]"
done

rm -r /tmp/PaperlessTest