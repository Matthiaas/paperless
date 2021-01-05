#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J workload
#BSUB -o output.o%J
#BSUB -W 04:00
#BSUB -n 40

# On cluster run: bsub < benchmark_cluster.sh

# Call with argument basic or workload
KEYLEN=16
VALLEN=(32) #, 64, 512)
COUNT=20000
UPDATE_RATIO=0
RANKS=(4 8 16) #8 16 20) # 40 80 160 320)
CORES=(2)
N_RUNS=10


export MAX_LOCAL_MEMTABLE_SIZE=10000000
export MAX_REMOTE_MEMTABLE_SIZE=10000000
export MAX_LOCAL_CACHE_SIZE=10000000
export MAX_REMOTE_CACHE_SIZE=10000000
export DISPATCH_IN_CHUNKS=0
export STORAGE_LOCATION=/tmp/mydb


for k in $(seq $N_RUNS); do
  for c in "${CORES[@]}"; do
    for i in "${RANKS[@]}"; do
        for j in "${VALLEN[@]}"; do
           echo mpirun --map-by node:PE=$c -np $i ./build/throughput_Ipaperless $KEYLEN $j $COUNT $UPDATE_RATIO /tmp/scratch/$USER/throughput/Ipaperless/runs$k/ranks$i
           mpirun --map-by node:PE=$c -np $i ./build/throughput_Ipaperless $KEYLEN $j $COUNT $UPDATE_RATIO /tmp/scratch/$USER/throughput/Ipaperless/runs$k/ranks$i
           #rm -r $STORAGE_LOCATION
        done
    done
  done

  for c in "${CORES[@]}"; do
    for i in "${RANKS[@]}"; do
        for j in "${VALLEN[@]}"; do
           echo mpirun --map-by node:PE=$c -np $i ./build/throughput_paperless $KEYLEN $j $COUNT $UPDATE_RATIO /tmp/scratch/$USER/throughput/paperless/runs$k/ranks$i
           mpirun --map-by node:PE=$c -np $i ./build/throughput_paperless $KEYLEN $j $COUNT $UPDATE_RATIO /tmp/scratch/$USER/throughput/paperless/runs$k/ranks$i
           #rm -r $STORAGE_LOCATION
        done
    done
  done
done
