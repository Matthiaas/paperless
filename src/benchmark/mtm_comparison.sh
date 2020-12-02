#!/usr/bin/env bash
# Call with argument basic or workload
KEYLEN=20
VALLEN=20
#                 1,000  100,000  1,000,000 kv pairs
MEM_TABLE_MAX_KB=(40000 4000000 40000000)
COUNT=100000
UPDATE_RATIO=(10 30 60 80)
NUM_PRODUCER_THREADS=(1 2 4 8) # 16 20 40 80 160 320)
NUM_CONSUMER_THREADS=(1 3) # 16 20 40 80 160 320)
SEED=0

for i in "${UPDATE_RATIO[@]}"; do
  for j in "${MEM_TABLE_MAX_KB[@]}"; do
      for k in "${NUM_PRODUCER_THREADS[@]}"; do
        for l in "${NUM_CONSUMER_THREADS[@]}"; do
          echo "UPDATE_RATIO=$i MEM_TABLE_MAX_KB=$j NUM_PRODUCER_THREADS=$k NUM_CONSUMER_THREADS=$l"
          ../../build/mtm_comparison $KEYLEN $VALLEN $COUNT $i $j $k $l $SEED
        done
      done 
  done
done
