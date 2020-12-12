#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J workload
#BSUB -o output.o%J
#BSUB -W 02:00
#BSUB -n 20

# On cluster run: bsub < benchmark_cluster.sh

# Call with argument basic or workload
KEYLEN=16
VALLEN=(8) #, 64, 512)
COUNT=10000
RANKS=(1) # 16 20) # 40 80 160 320)


export MAX_LOCAL_MEMTABLE_SIZ=100000
export MAX_REMOTE_MEMTABLE_SIZE=100000
export MAX_LOCAL_CACHE_SIZE=100000
export MAX_REMOTE_CACHE_SIZE=100000
export DISPATCH_IN_CHUNKS=1
export STORAGE_LOCATION=/scratch/mydb




for i in "${RANKS[@]}"; do
    for j in "${VALLEN[@]}"; do
        perf record -call-graph dwarf mpirun -np $i ./build/workload $KEYLEN $j $COUNT 0
    done
done
