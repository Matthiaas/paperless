#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J workload
#BSUB -o output.o%J
#BSUB -W 04:00
#BSUB -n 40

# On cluster run: bsub < benchmark_cluster.sh

# Call with argument basic or workload
KEYLEN=16
VALLEN=(8) #, 64, 512)
COUNT=10000
UPDATE_RATIO=0
RANKS=(16) #8 16 20) # 40 80 160 320)


export MAX_LOCAL_MEMTABLE_SIZE=10000000
export MAX_REMOTE_MEMTABLE_SIZE=10000000
export MAX_LOCAL_CACHE_SIZE=10000000
export MAX_REMOTE_CACHE_SIZE=10000000
export DISPATCH_IN_CHUNKS=1
export STORAGE_LOCATION=/tmp/mydb




for i in "${RANKS[@]}"; do
    for j in "${VALLEN[@]}"; do
       mpirun --use-hwthread-cpus -np $i ./build/thegreatbenchmark_paperless $KEYLEN $j $COUNT $UPDATE_RATIO /home/matthias/Documents/eth/dphpc/paperless/analytics/data/localRunIprobe/$i
       #mpirun -np $i ./build/thegreatbenchmark_papyrus $KEYLEN $j $COUNT $UPDATE_RATIO /cluster/scratch/$USER/512MB/papyrus/$i
    done
done

