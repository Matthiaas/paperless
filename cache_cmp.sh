#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J workload
#BSUB -o output.o%J
#BSUB -W 04:00
#BSUB -n 48

# On cluster run: bsub < benchmark_cluster.sh


KILO=1024
MEGA=$((1024 * KILO))
GIGA=$((1024 * MEGA))


# Call with argument basic or workload
KEYLEN=16
VALLEN=(8 32 512 1024 4096)
COUNT=1000000
UPDATE_RATIO=0
RANKS=(1 4 8 16) #8 16 20) # 40 80 160 320)
CORES=(2)


export MAX_LOCAL_MEMTABLE_SIZE=$GIGA
export MAX_REMOTE_MEMTABLE_SIZE=$GIGA
export MAX_LOCAL_CACHE_SIZE=$GIGA
export MAX_REMOTE_CACHE_SIZE=$GIGA
export DISPATCH_IN_CHUNKS=0
export STORAGE_LOCATION=/scratch/mydb/

for c in "${CORES[@]}"; do
  for i in "${RANKS[@]}"; do
      for j in "${VALLEN[@]}"; do
         echo mpirun --map-by node:PE=$c -np $i ./build/thegreatbenchmark_paperless $j $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/cache_comp/tree/"${j}"ksize"${j}"vsize/ranks$i
         mpirun --map-by node:PE=$c -np $i ./build/thegreatbenchmark_paperless $j $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/cache_comp/tree/"${j}"ksize"${j}"vsize/ranks$i
      done
  done
done


for c in "${CORES[@]}"; do
  for i in "${RANKS[@]}"; do
      for j in "${VALLEN[@]}"; do
         echo mpirun --map-by node:PE=$c -np $i ./build/thegreatbenchmark_paperless_hash_cache $j $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/cache_comp/hash/"${j}"ksize"${j}"vsize/ranks$i
         mpirun --map-by node:PE=$c -np $i ./build/thegreatbenchmark_paperless $j $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/cache_comp/hash/"${j}"ksize"${j}"vsize/ranks$i
      done
  done
done
