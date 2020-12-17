#!/usr/bin/env bash

MAIN_DIR=$(dirname "$0")/../..
OUT_DIR=/tmp/paperless_bench


# Check if we're on a cluster.
[ -d "/cluster" ]
if [ $? -eq 0 ]
  then
    OUT_DIR=/cluster/scratch/$USER/run/paperless
fi

mkdir -p ${OUT_DIR}
VEC_OUTFILE=${OUT_DIR}/result_cmp_vectorized.txt
MCMP_OUTFILE=${OUT_DIR}/result_cmp_memcmp.txt

cd ${MAIN_DIR} || exit

rm build/* -rf
./build.sh cmp_vector RELEASE -DVECTORIZE=ON
./build/cmp_vector > ${VEC_OUTFILE}

./build.sh cmp_vector RELEASE -DVECTORIZE=OFF
./build/cmp_vector > ${MCMP_OUTFILE}
