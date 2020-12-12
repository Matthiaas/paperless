#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J workload
#BSUB -o output.o%J
#BSUB -W 04:00
#BSUB -n 40

RANKS=(1 8 16 32) # 40 80 160 320)



for i in "${RANKS[@]}"; do
	echo $i
	mpirun -n $i ../../build/mpi_probe_multiple_threads
done

