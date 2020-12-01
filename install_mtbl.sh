#!/usr/bin/env bash

# Installs mtbl on cluster to ~/usr

if [ ! -d "/cluster" ]
  then
  echo "You probably don't want to run this script outside of the cluster."
  exit 1
fi

orig_dir=$(pwd)
cd /tmp
git clone https://github.com/farsightsec/mtbl
cd mtbl

source $orig_dir/load_modules.sh
./autogen.sh
./configure --prefix=$HOME/usr
make install

# Cleanup
rm -rf /tmp/mtbl
