#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J througput
#BSUB -R "rusage[scratch=5120] span[hosts=1]"
#BSUB -o out.througput.o%J
#BSUB -W 04:00
#BSUB -n 48

KILO=1024
MEGA=$((1024 * KILO))
GIGA=$((1024 * MEGA))

# On cluster run: bsub < benchmark_cluster.sh

# Call with argument basic or workload
KEYLEN=16
VALLEN=(64 512 13000)
COUNT=10000
batch_sizes=(100 1000 10000) # should be smaller than count
UPDATE_RATIO=0
RANKS=(4 8 16 20 24) # 40 80 160 320)

CORES=(2)
N_RUNS=30


export MAX_LOCAL_MEMTABLE_SIZE=10000000000
export MAX_REMOTE_MEMTABLE_SIZE=10000000000
export MAX_LOCAL_CACHE_SIZE=10000000000
export MAX_REMOTE_CACHE_SIZE=10000000000
export DISPATCH_IN_CHUNKS=1


EXPERIMENT=throughput_one_host
export STORAGE_LOCATION=/scratch/$EXPERIMENT/
DATA_LOCATION=/cluster/scratch/$USER/$EXPERIMENT

for k in $(seq $N_RUNS); do
  for c in "${CORES[@]}"; do
    for i in "${RANKS[@]}"; do
        for j in "${VALLEN[@]}"; do
          for b in "${batch_sizes[@]}"; do
             echo mpirun --map-by node:PE=$c -np $i ./build/throughput_Ipaperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/Ipaperless/runs$k/ranks$i REL $b
             mpirun --map-by node:PE=$c -np $i ./build/throughput_Ipaperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/Ipaperless/runs$k/ranks$i REL $b
           done
        done
    done
  done

  for c in "${CORES[@]}"; do
    for i in "${RANKS[@]}"; do
        for j in "${VALLEN[@]}"; do
          for b in "${batch_sizes[@]}"; do
             echo mpirun --map-by node:PE=$c -np $i ./build/throughput_paperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/paperless/runs$k/ranks$i REL $b
             mpirun --map-by node:PE=$c -np $i ./build/throughput_paperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/paperless/runs$k/ranks$i REL $b
           done
        done
    done
  done

  for c in "${CORES[@]}"; do
    for i in "${RANKS[@]}"; do
        for j in "${VALLEN[@]}"; do
          for b in "${batch_sizes[@]}"; do
             echo mpirun --map-by node:PE=$c -np $i ./build/throughput_papyrus $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/paperless/runs$k/ranks$i REL $b
             mpirun --map-by node:PE=$c -np $i ./build/throughput_papyrus $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/paperless/runs$k/ranks$i REL $b
          done
        done
    done
  done
done
