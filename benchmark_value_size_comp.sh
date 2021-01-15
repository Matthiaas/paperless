#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J size_comp
#BSUB -R "rusage[scratch=5120,mem=5120] span[ptile=2] select[model==EPYC_7742]"
#BSUB -o out.size_comp.o%J
#BSUB -W 04:00
#BSUB -n 32

# On cluster run: bsub < benchmark_value_size_comp.sh
KILO=1024
MEGA=$((1024 * KILO))
GIGA=$((1024 * MEGA))

CONSTLEN=16
VALLEN=(256 $KILO $((4 * KILO)) $((16 * KILO)) $((64 * KILO)) $((256 * KILO)) $((MEGA)))
COUNT=5000
UPDATE_RATIO=0
N_RUNS=10
RANKS=16

export MAX_LOCAL_MEMTABLE_SIZE=$GIGA
# MAX_REMOTE_MEMTABLE_SIZE has to be set to MAX_LOCAL_MEMTABLE_SIZE because papyrus uses
# one param to set both sizes.
export MAX_REMOTE_MEMTABLE_SIZE=$MAX_LOCAL_MEMTABLE_SIZE
export MAX_LOCAL_CACHE_SIZE=$GIGA
# Same as for memtable size.
export MAX_REMOTE_CACHE_SIZE=$MAX_LOCAL_CACHE_SIZE
export DISPATCH_IN_CHUNKS=1
EXPERIMENT=val_size_cmp
export STORAGE_LOCATION=/scratch/$EXPERIMENT
export DATA_LOCATION=/cluster/scratch/$USER/$EXPERIMENT



for k in $(seq $N_RUNS); do
  for j in "${VALLEN[@]}"; do
    echo "val$j/run$k"
    echo mpirun --map-by node:PE=2 -np $RANKS ./build/thegreatbenchmark_paperless $CONSTLEN $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/$EXPERIMENT/"${CONSTLEN}"ksize"${j}"vsize/ranks$RANKS/run$k SEQ
    mpirun --map-by node:PE=2 -np $RANKS ./build/thegreatbenchmark_paperless $CONSTLEN $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/$EXPERIMENT/"${CONSTLEN}"ksize"${j}"vsize/ranks$RANKS/run$k SEQ
  done
done

#for k in $(seq $N_RUNS); do
#  for j in "${VALLEN[@]}"; do
#    echo "val$j/run$k"
#    echo mpirun --map-by node:PE=2 -np $RANKS ./build/thegreatbenchmark_paperless $j $CONSTLEN $COUNT $UPDATE_RATIO /cluster/scratch/$USER/size_comp/"${j}"ksize"${CONSTLEN}"vsize/ranks$RANKS/run$k SEQ
#    mpirun --map-by node:PE=2 -np $RANKS ./build/thegreatbenchmark_paperless $j $CONSTLEN $COUNT $UPDATE_RATIO /cluster/scratch/$USER/size_comp/"${j}"ksize"${CONSTLEN}"vsize/ranks$RANKS/run$k SEQ
#  done
#done
