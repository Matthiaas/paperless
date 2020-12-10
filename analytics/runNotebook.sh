#!/usr/bin/env bash

export PAPERLESS_KV_DATA_DIR=$PWD"/""$(dirname "$BASH_SOURCE")"
ipython notebook $PAPERLESS_KV_DATA_DIR/KV_plots.ipynb