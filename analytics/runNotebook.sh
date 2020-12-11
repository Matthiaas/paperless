#!/usr/bin/env bash

# This is a magic line from SO:
# 	https://stackoverflow.com/a/58081608
eval "$(command conda 'shell.bash' 'hook' 2> /dev/null)"
conda activate paperless
export PAPERLESS_KV_DATA_DIR=$PWD"/""$(dirname "$BASH_SOURCE")"

jupyter notebook $PAPERLESS_KV_DATA_DIR/KV_plots.ipynb
