#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J workload
#BSUB -o output.o%J
#BSUB -W 04:00
#BSUB -n 40

RANKS=(1 4) #16 32) # 40 80 160 320)
CORES=(1 2 3)

for i in "${RANKS[@]}"; do
  for c in "${CORES[@]}"; do
    echo "Ranks" $i
    echo "Cores per rank" $c
    echo mpirun --map-by node:PE=$c -n $i ../../build/mpi_probe_multiple_threads
    mpirun --map-by node:PE=$c -n $i ../../build/mpi_probe_multiple_threads
    echo ""
  done
done


 echo "-----------------------------------------------------------------------"
 echo "Yield on Idle:"  --mca mpi_yield_when_idle 1
 echo "-----------------------------------------------------------------------"
 echo ""

 for i in "${RANKS[@]}"; do
  for c in "${CORES[@]}"; do
    echo "Ranks" $i
    echo "Cores per rank" $c
    echo mpirun --map-by node:PE=$c --mca mpi_yield_when_idle 1 -n $i ../../build/mpi_probe_multiple_threads
    mpirun --map-by node:PE=$c --mca mpi_yield_when_idle 1 -n $i ../../build/mpi_probe_multiple_threads
    echo ""
  done
done