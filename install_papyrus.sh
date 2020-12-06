#!/usr/bin/env bash

# Clones and compiles papyrus
if [ -d "/cluster" ]
  then
    . ./load_modules.sh
fi

rm -rf papyrus
git clone https://code.ornl.gov/eck/papyrus.git
cd papyrus

# Disable Fortran bindings, otherwise it gives some weird errors.
sed -i "s/DPAPYRUS_USE_FORTRAN=ON/DPAPYRUS_USE_FORTRAN=OFF/" build.sh

./build.sh
