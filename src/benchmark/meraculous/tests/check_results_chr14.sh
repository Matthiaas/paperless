#!/bin/bash

set -e

ref_path=$(cd $(dirname $0) && pwd)

echo "extracting contigs ..."
egrep -h -A 1 '^>Contig' ../output_*_human-chr14* \
	| egrep -v '(^>|^-)' \
	> tmp.contigs.txt

echo "applying dictionary sort ..."
sort -d < tmp.contigs.txt > tmp.contigs.sorted.txt

echo "checking sorted contigs ..."
if diff -q $ref_path/human-chr14.contigs.no_circular.sorted.txt tmp.contigs.sorted.txt; then
	echo "test: PASS"
	rm -f tmp.contigs.txt tmp.contigs.sorted.txt
else
	echo "test: FAIL"
	exit 1
fi
