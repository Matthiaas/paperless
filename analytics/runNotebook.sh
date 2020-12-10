#!/usr/bin/env bash

export PAPERLESS_KV_DATA_DIR=$PWD"/""$(dirname "$BASH_SOURCE")"
ipython3 notebook $PAPERLESS_KV_DATA_DIR/KV_plots.ipynb
