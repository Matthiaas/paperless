#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J op_time
#BSUB -R "rusage[scratch=5120,mem=5120] span[hosts=1] select[model==EPYC_7742]"
#BSUB -o out.op_time.o%J
#BSUB -W 04:00
#BSUB -n 48

# On cluster run: bsub < benchmark_op_times.sh

KILO=1024
MEGA=$((1024 * KILO))
GIGA=$((1024 * MEGA))

KEYLEN=16
VALLEN=$((128 * KILO))
COUNT=1000
UPDATE_RATIO=(0 50 5)
RANKS=(1 2 4 8 12 16 24)
N_RUNS=10
#UPDATE_RATIO=(5)
#RANKS=(1 2)
#N_RUNS=3

# Paperless parameters.

export MAX_LOCAL_MEMTABLE_SIZE=$GIGA
# MAX_REMOTE_MEMTABLE_SIZE has to be set to MAX_LOCAL_MEMTABLE_SIZE because papyrus uses
# one param to set both sizes.
export MAX_REMOTE_MEMTABLE_SIZE=$MAX_LOCAL_MEMTABLE_SIZE
export MAX_LOCAL_CACHE_SIZE=$GIGA
# Same as for memtable size.
export MAX_REMOTE_CACHE_SIZE=$MAX_LOCAL_CACHE_SIZE
export DISPATCH_IN_CHUNKS=1
EXPERIMENT=opt_time_report_one_host
export STORAGE_LOCATION=/scratch/$EXPERIMENT
DATA_LOCATION=/cluster/scratch/$USER/$EXPERIMENT

# Papyrus parameters.
# Enable usage of caches.
export PAPYRUSKV_CACHE_LOCAL=1
export PAPYRUSKV_CACHE_REMOTE=1
export PAPYRUSKV_MEMTABLE_SIZE=$MAX_LOCAL_MEMTABLE_SIZE
# Remote buffer size is kept default (128KB), as Paperless doesn't have such a thing.
# We are benchmarking with 128KB values, so the KV pairs are quite big already and with
# the default settings the remote buffer gets disabled anyway.
# export PAPYRUSKV_REMOTE_BUFFER_SIZE=...
# PAPYRUSKV_REMOTE_BUFFER_ENTRY_MAX -- max size of value in remote buffer, if value size is specified. Default 4KB
export PAPYRUSKV_CACHE_SIZE=$MAX_LOCAL_CACHE_SIZE
export PAPYRUSKV_DESTROY_REPOSITORY=1 # Gets rid of the data afterwards.


mpirun --version
MPIRUN_FLAGS=("--report-bindings" "--map-by" "node:pe=2")

rm -rf ${STORAGE_LOCATION}*
for k in $(seq $N_RUNS); do
  for i in "${RANKS[@]}"; do
    for j in "${UPDATE_RATIO[@]}"; do
      echo "ratio$j/ranks$i/run$k"
      PAPERLESS_PATH=$DATA_LOCATION/paperless/ratio$j/ranks$i/run$k
      PAPYRUS_PATH=$DATA_LOCATION/papyrus/ratio$j/ranks$i/run$k
      mkdir -p $PAPYRUS_PATH
      mkdir -p $PAPERLESS_PATH
      echo mpirun -np $i ${MPIRUN_FLAGS[@]} ./build/thegreatbenchmark_paperless $KEYLEN $VALLEN $COUNT $j $PAPERLESS_PATH SEQ
      mpirun -np $i ${MPIRUN_FLAGS[@]} ./build/thegreatbenchmark_paperless $KEYLEN $VALLEN $COUNT $j $PAPERLESS_PATH SEQ
      echo mpirun -np $i ${MPIRUN_FLAGS[@]} ./build/thegreatbenchmark_papyrus $KEYLEN $VALLEN $COUNT $j $PAPYRUS_PATH SEQ
      mpirun -np $i ${MPIRUN_FLAGS[@]} ./build/thegreatbenchmark_papyrus $KEYLEN $VALLEN $COUNT $j $PAPYRUS_PATH SEQ
      echo ""
    done
  done
done
