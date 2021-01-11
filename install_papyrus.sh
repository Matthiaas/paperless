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
sed -i "32iset_property(TARGET papyruskv PROPERTY POSITION_INDEPENDENT_CODE ON)" kv/src/CMakeLists.txt

./build.sh
