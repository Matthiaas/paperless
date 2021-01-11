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
VALLEN=(64 2048 131072)
COUNT=5000
UPDATE_RATIOS=(0 5 50)
RANKS=(1 4 8 16 20 24) # 40 80 160 320)

CORES=(2)
N_RUNS=30


export MAX_LOCAL_MEMTABLE_SIZE=10000000000
export MAX_REMOTE_MEMTABLE_SIZE=10000000000
export MAX_LOCAL_CACHE_SIZE=10000000000
export MAX_REMOTE_CACHE_SIZE=10000000000
export DISPATCH_IN_CHUNKS=1


EXPERIMENT=throughput_report_one_host
export STORAGE_LOCATION=/scratch/$EXPERIMENT
DATA_LOCATION=/cluster/scratch/$USER/$EXPERIMENT


# Papyrus parameters.
# Enable usage of caches.
export PAPYRUSKV_CACHE_LOCAL=1
export PAPYRUSKV_CACHE_REMOTE=1
export PAPYRUSKV_MEMTABLE_SIZE=$MAX_LOCAL_MEMTABLE_SIZE
export PAPYRUSKV_CACHE_SIZE=$MAX_LOCAL_CACHE_SIZE
export PAPYRUSKV_DESTROY_REPOSITORY=1 # Gets rid of the data afterwards.




rm -r DATA_LOCATION

for k in $(seq $N_RUNS); do
  for c in "${CORES[@]}"; do
    for i in "${RANKS[@]}"; do
        for j in "${VALLEN[@]}"; do
          for UPDATE_RATIO in "${UPDATE_RATIOS[@]}"; do
             echo mpirun --map-by node:PE=$c -np $i ./build/throughput_Ipaperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/Ipaperless REL 1000
             mpirun --map-by node:PE=$c -np $i ./build/throughput_Ipaperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/Ipaperless REL 1000
           done
        done
    done
  done

  for c in "${CORES[@]}"; do
    for i in "${RANKS[@]}"; do
        for j in "${VALLEN[@]}"; do
          for UPDATE_RATIO in "${UPDATE_RATIOS[@]}"; do
             echo mpirun --map-by node:PE=$c -np $i ./build/throughput_paperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/paperless REL 1000
             mpirun --map-by node:PE=$c -np $i ./build/throughput_paperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/paperless REL 1000
           done
        done
    done
  done

  for c in "${CORES[@]}"; do
    for i in "${RANKS[@]}"; do
        for j in "${VALLEN[@]}"; do
          for UPDATE_RATIO in "${UPDATE_RATIOS[@]}"; do
             echo mpirun --map-by node:PE=$c -np $i ./build/throughput_papyrus $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/papyrus REL 1000
             mpirun --map-by node:PE=$c -np $i ./build/throughput_papyrus $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/papyrus REL 1000
          done
        done
    done
  done
done
