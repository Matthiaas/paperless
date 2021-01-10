#!/usr/bin/env bash
#BSUB -P paperless
#BSUB -J paperless_meraculous
#BSUB -R "rusage[mem=10240] span[hosts=1]"
#BSUB -o out.meraculous.%J
#BSUB -W 00:15
#BSUB -n 48

# On cluster run: bsub < artificial_workload_benchmark.sh

echo "Hosts: "
echo $LSB_HOSTS

export UPC_NODES=$LSB_HOSTS

SHARED_HEAP=1G

KMER_LENGTH=51

ufx=/cluster/home/jbazinska/paperless/src/benchmark/meraculous/tests/human-chr14.contigs.no_circular.sorted.txt

tag=human-25pct # $PBS_JOBID

required_flags="-m 51 -c 100 -l 1 -d 2"

export GASNET_PHYSMEM_MAX=100G
export GASNET_BACKTRACE=1

upcrun -q -n 24 -shared-heap=$SHARED_HEAP \
       ./meraculous-upc-$KMER_LENGTH \
               $required_flags \
               -i $ufx \
               -o $tag \
               > stats.papyrus.$tag

#upcrun -q -n 24 -shared-heap=$SHARED_HEAP \
#       ./meraculous-papyrus-$KMER_LENGTH \
#               $required_flags \
#               -i $ufx \
#               -o $tag \
#               > stats.papyrus.$tag

#upcrun -q -n 24 -shared-heap=$SHARED_HEAP \
#       ./meraculous-$KMER_LENGTH \
#               $required_flags \
#               -i $ufx \
#               -o $tag \
#               > stats.$tag
