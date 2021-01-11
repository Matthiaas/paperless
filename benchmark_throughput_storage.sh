#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J storage
#BSUB -R "rusage[scratch=5120] span[ptile=2] select[model==EPYC_7742]"
#BSUB -o out.storage.o%J
#BSUB -W 04:00
#BSUB -n 48

KILO=1024
MEGA=$((1024 * KILO))
GIGA=$((1024 * MEGA))

# On cluster run: bsub < benchmark_cluster.sh

# Call with argument basic or workload
KEYLEN=16
VALLEN=(65536) # 64 KB
COUNT=10000
RANKS=(16)

# 1%, 25% ,and 100% of the data.
MEM_TABLE_SIZES=(6555200 173880000 665520000)

CORES=(2)
N_RUNS=30


export MAX_REMOTE_MEMTABLE_SIZE=10000000000
export MAX_LOCAL_CACHE_SIZE=10000000000
export MAX_REMOTE_CACHE_SIZE=10000000000
export DISPATCH_IN_CHUNKS=1


EXPERIMENT=throughput_storage_report
export STORAGE_LOCATION=/scratch/$EXPERIMENT
DATA_LOCATION=/cluster/scratch/$USER/$EXPERIMENT


# Papyrus parameters.
# Enable usage of caches.
export PAPYRUSKV_CACHE_LOCAL=1
export PAPYRUSKV_CACHE_REMOTE=1
export PAPYRUSKV_CACHE_SIZE=$MAX_LOCAL_CACHE_SIZE
export PAPYRUSKV_DESTROY_REPOSITORY=1 # Gets rid of the data afterwards.




rm -r DATA_LOCATION

for k in $(seq $N_RUNS); do

  for c in "${CORES[@]}"; do
    for i in "${RANKS[@]}"; do
        for j in "${VALLEN[@]}"; do
          for MEM_TBL_SIZE in "${MEM_TABLE_SIZES[@]}"; do

            export MAX_LOCAL_MEMTABLE_SIZE=$MEM_TBL_SIZE
            export PAPYRUSKV_MEMTABLE_SIZE=$MAX_LOCAL_MEMTABLE_SIZE


             echo mpirun --map-by node:PE=$c -np $i ./build/throughput_paperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/paperless_$MEM_TBL_SIZE REL 0 CHECKPOINT
             mpirun --map-by node:PE=$c -np $i ./build/throughput_paperless $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/paperless_$MEM_TBL_SIZE REL 0 CHECKPOINT


             echo mpirun --map-by node:PE=$c -np $i ./build/throughput_papyrus $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/papyrus_$MEM_TBL_SIZE REL 0 CHECKPOINT
             mpirun --map-by node:PE=$c -np $i ./build/throughput_papyrus $KEYLEN $j $COUNT $UPDATE_RATIO $DATA_LOCATION/papyrus_$MEM_TBL_SIZE REL 0 CHECKPOINT

             echo ""
           done
        done
    done
  done
done
