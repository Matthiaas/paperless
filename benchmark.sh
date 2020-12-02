#!/usr/bin/env bash
# Call with argument basic or workload
KEYLEN=16
VALLEN=(8) #, 64, 512)
COUNT=10000
RANKS=(1 2 4)  #8 16 20 40 80 160 320)


for i in "${RANKS[@]}"; do
    for j in "${VALLEN[@]}"; do
        mpirun -np $i ./build/$1 $KEYLEN $j $COUNT 0
        for ((j=0; j <$i; j++))
        do
          rm -r /tmp/mydb${j}
        done
    done
done
