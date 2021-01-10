#!/bin/bash

. ./load_modules.sh

echo "Installing Berkeley UPC."
rm berkeley_upc-2.26.0.tar.gz
rm -rf berkeley_upc-2.26.0
wget https://upc.lbl.gov/download/release/berkeley_upc-2.26.0.tar.gz
tar zxf berkeley_upc-2.26.0.tar.gz
cd berkeley_upc-2.26.0
mkdir -p build && cd build

../configure --prefix=$HOME/berkeley_upc
make -j 8
make install
export PATH=$HOME/berkeley_upc/bin:$PATH

echo "Please source the script -- otherwise your path won't be updated."

