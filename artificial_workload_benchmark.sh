#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J artificial_workload
#BSUB -R "rusage[scratch=5120] span[hosts=1]"
#BSUB -o out.artificial_workload.o%J
#BSUB -W 04:00
#BSUB -n 40

# On cluster run: bsub < artificial_workload_benchmark.sh

KILO=1024
MEGA=$((1024 * KILO))
GIGA=$((1024 * MEGA))

KEYLEN=16
VALLEN=$((128 * KILO))
COUNT=1000
UPDATE_RATIO=(0 50 5)
RANKS=(1 2 4 8 12 16)
N_RUNS=10
#UPDATE_RATIO=(5)
#RANKS=(1 2)
#N_RUNS=3

export MAX_LOCAL_MEMTABLE_SIZE=$GIGA
export MAX_REMOTE_MEMTABLE_SIZE=$GIGA
export MAX_LOCAL_CACHE_SIZE=$GIGA
export MAX_REMOTE_CACHE_SIZE=$GIGA
export DISPATCH_IN_CHUNKS=1
EXPERIMENT=artificial_workload
export STORAGE_LOCATION=/cluster/scratch/$USER/$EXPERIMENT/checkpoints
DATA_LOCATION=/cluster/scratch/$USER/$EXPERIMENT
#export STORAGE_LOCATION=/home/julia/eth/dphpc/paperless/analytics/data/$EXPERIMENT/checkpoints
#DATA_LOCATION=/home/julia/eth/dphpc/paperless/analytics/data/$EXPERIMENT

mpirun --version

rm -rf ${STORAGE_LOCATION}*
for i in "${RANKS[@]}"; do
  for j in "${UPDATE_RATIO[@]}"; do
    for k in $(seq $N_RUNS); do
      echo "ratio$j/ranks$i/run$k"
      PAPERLESS_PATH=$DATA_LOCATION/paperless/ratio$j/ranks$i/run$k
      PAPYRUS_PATH=$DATA_LOCATION/papyrus/ratio$j/ranks$i/run$k
      mkdir -p $PAPYRUS_PATH
      mkdir -p $PAPERLESS_PATH
      echo mpirun -np $i ./build/thegreatbenchmark_paperless $KEYLEN $VALLEN $COUNT $j $PAPERLESS_PATH
      mpirun -np $i ./build/thegreatbenchmark_paperless $KEYLEN $VALLEN $COUNT $j $PAPERLESS_PATH
      rm -rf ${STORAGE_LOCATION}*
      echo mpirun -np $i ./build/thegreatbenchmark_papyrus $KEYLEN $VALLEN $COUNT $j $PAPYRUS_PATH
      mpirun -np $i ./build/thegreatbenchmark_papyrus $KEYLEN $VALLEN $COUNT $j $PAPYRUS_PATH
      rm -rf ${STORAGE_LOCATION}*
    done
  done
done
