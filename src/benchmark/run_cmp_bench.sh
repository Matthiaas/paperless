#!/usr/bin/env bash

MAIN_DIR=$(dirname "$0")/../..
OUT_DIR=${MAIN_DIR}/analytics/data/compare_vec

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


# Build and run vectorized version
rm build/* -rf
./build.sh cmp_vector RELEASE -DVECTORIZE=ON
mv ./build/cmp_vector ./build/cmp_vector_vectorized
./build/cmp_vector_vectorized > ${VEC_OUTFILE}

# Build and run memcmp version
rm build/* -rf
./build.sh cmp_vector RELEASE -DVECTORIZE=OFF
mv ./build/cmp_vector ./build/cmp_vector_memcmp
./build/cmp_vector_memcmp > ${MCMP_OUTFILE}
