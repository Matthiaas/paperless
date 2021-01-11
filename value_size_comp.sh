#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J size_comp
#BSUB -o output.o%J
#BSUB -W 04:00
#BSUB -n 48

# On cluster run: bsub < benchmark_cluster.sh


KILO=1024
MEGA=$((1024 * KILO))
GIGA=$((1024 * MEGA))


# Call with argument basic or workload
KEYLEN=16
VALLEN=(8 32 64 256 512 1024 2048 4096) #, 64, 512)
COUNT=4000
UPDATE_RATIO=0
RANKS=(1 2 8 16 32) #8 16 20) # 40 80 160 320)
CORES=(1 2)



export MAX_LOCAL_MEMTABLE_SIZE=$GIGA
export MAX_REMOTE_MEMTABLE_SIZE=$GIGA
export MAX_LOCAL_CACHE_SIZE=$GIGA
export MAX_REMOTE_CACHE_SIZE=$GIGA
export DISPATCH_IN_CHUNKS=1
export STORAGE_LOCATION=/cluster/mydb_val_size_cmp

for c in "${CORES[@]}"; do
  for i in "${RANKS[@]}"; do
      for j in "${VALLEN[@]}"; do
         echo mpirun --map-by node:PE=$c -np $i ./build/thegreatbenchmark_paperless $KEYLEN $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/size_comp/"${KEYLEN}"ksize"${j}"vsize/ranks$i
         mpirun --map-by node:PE=$c -np $i ./build/thegreatbenchmark_paperless $KEYLEN $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/size_comp/"${KEYLEN}"ksize"${j}"vsize/ranks$i
      done
  done
done

for c in "${CORES[@]}"; do
  for i in "${RANKS[@]}"; do
      for j in "${VALLEN[@]}"; do
         echo mpirun --map-by node:PE=$c -np $i ./build/thegreatbenchmark_paperless $j $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/size_comp/"${j}"ksize"${j}"vsize/ranks$i
         mpirun --map-by node:PE=$c -np $i ./build/thegreatbenchmark_paperless $j $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/size_comp/"${j}"ksize"${j}"vsize/ranks$i
      done
  done
done
