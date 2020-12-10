#!/usr/bin/env bash

conda activate paperless
export PAPERLESS_KV_DATA_DIR=$PWD"/""$(dirname "$BASH_SOURCE")"

jupyter notebook $PAPERLESS_KV_DATA_DIR/KV_plots.ipynb
