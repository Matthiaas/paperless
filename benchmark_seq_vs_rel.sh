#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J seq_vs_rel
#BSUB -R "rusage[scratch=5120] span[hosts=1] select[model==EPYC_7742]"
#BSUB -o out.seq_vs_rel.o%J
#BSUB -W 04:00
#BSUB -n 48

# On cluster run: bsub < benchmark_seq_vs_rel.sh

KILO=1024
MEGA=$((1024 * KILO))
GIGA=$((1024 * MEGA))

KEYLEN=16
VALLEN=$((64 * KILO))
COUNT=10000
UPDATE_RATIO=(0)
RANKS=(1 4 8 16 24)
N_RUNS=30

# 1%, 25% ,and 100% of the data.
MEM_TABLE_SIZES=(6555200 173880000 665520000)

# Toggle between "paperless" and "papyrus" 
KV="paperless"

export MAX_LOCAL_MEMTABLE_SIZE=$GIGA
export MAX_REMOTE_MEMTABLE_SIZE=$GIGA
export MAX_LOCAL_CACHE_SIZE=$GIGA
export MAX_REMOTE_CACHE_SIZE=$GIGA
export DISPATCH_IN_CHUNKS=1
EXPERIMENT=seq_vs_rel_2core_report_one_host
export STORAGE_LOCATION=/scratch/$EXPERIMENT
DATA_LOCATION=/cluster/scratch/$USER/$EXPERIMENT

mpirun --version
MPIRUN_FLAGS=("--report-bindings" "--map-by node:pe=2")

rm -rf ${STORAGE_LOCATION}*

for k in $(seq $N_RUNS); do
  for i in "${RANKS[@]}"; do
    for j in "${UPDATE_RATIO[@]}"; do

      echo "ratio$j/ranks$i/run$k"
      REL_PATH=$DATA_LOCATION/$KV/rel/ratio$j/ranks$i/run$k
      SEQ_PATH=$DATA_LOCATION/$KV/seq/ratio$j/ranks$i/run$k
      mkdir -p $REL_PATH
      mkdir -p $SEQ_PATH

      for MEM_TBL_SIZE in "${MEM_TABLE_SIZES[@]}"; do

        export MAX_REMOTE_MEMTABLE_SIZE=$MEM_TBL_SIZE
        echo mpirun -np $i ${MPIRUN_FLAGS[@]} ./build/thegreatbenchmark_$KV $KEYLEN $VALLEN $COUNT $j $REL_PATH REL
        mpirun -np $i ${MPIRUN_FLAGS[@]} ./build/thegreatbenchmark_$KV $KEYLEN $VALLEN $COUNT $j $REL_PATH REL
      done

      echo mpirun -np $i ${MPIRUN_FLAGS[@]} ./build/thegreatbenchmark_$KV $KEYLEN $VALLEN $COUNT $j $SEQ_PATH SEQ
      mpirun -np $i ${MPIRUN_FLAGS[@]}  ./build/thegreatbenchmark_$KV $KEYLEN $VALLEN $COUNT $j $SEQ_PATH SEQ

      echo ""
    done
  done
done
