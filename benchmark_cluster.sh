#!/usr/bin/env bash

#BSUB -P paperless
#BSUB -J workload
#BSUB -o output.o%J
#BSUB -W 5
#BSUB -n 3

# On cluster run: bsub < benchmark_cluster.sh
KEYLEN=16
VALLEN=64
COUNT=1000


export MAX_LOCAL_MEMTABLE_SIZE=100000
export MAX_REMOTE_MEMTABLE_SIZE=100000
export MAX_LOCAL_CACHE_SIZE=100000
export MAX_REMOTE_CACHE_SIZE=100000
export DISPATCH_IN_CHUNKS=1
export STORAGE_LOCATION=/scratch/mydb/

# -np 3 should not be needed
mpirun -np 3 ./build/workload $KEYLEN $VALLEN $COUNT 50
