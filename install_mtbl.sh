#!/usr/bin/env bash

# Installs mtbl on cluster to ~/usr

if [ ! -d "/cluster" ]
  then
  echo "You probably don't want to run this script outside of the cluster."
  exit 1
fi

cd /tmp
git clone https://github.com/farsightsec/mtbl
cd mtbl

# Load modules
. /cluster/apps/local/env2lmod.sh
module load libtool/2.4.6 lz4/1.8.1.2 snappy/1.1.7 zstd/1.3.0
./autogen.sh
./configure --prefix=$HOME/usr
make install

# Cleanup
rm -rf /tmp/mtbl
