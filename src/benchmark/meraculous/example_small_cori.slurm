#!/bin/bash

#SBATCH -C haswell
#SBATCH -p regular
#SBATCH -N 4
#SBATCH -t 00:30:00
#SBATCH -L SCRATCH
#SBATCH -J human-chr14
#SBATCH --qos=premium
##DW jobdw capacity=10GB access_mode=striped type=scratch

module load PrgEnv-intel
module load bupc-narrow

SHARED_HEAP=192M

KMER_LENGTH=51

ufx=$SCRATCH/Meraculous_data/human-chr14.txt.ufx.bin
tag=human-chr14_$SLURM_JOB_ID

required_flags="-m 51 -c 100 -l 1 -d 4"

date
upcrun -q -n 72 -shared-heap=$SHARED_HEAP \
	./meraculous-$KMER_LENGTH \
		$required_flags \
		-i $ufx \
		-o $tag
date
