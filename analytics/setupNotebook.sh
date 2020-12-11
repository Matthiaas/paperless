#!/usr/bin/env bash
conda env create -f environment.yml
# This is a magic line from SO:
# 	https://stackoverflow.com/a/58081608
eval "$(command conda 'shell.bash' 'hook' 2> /dev/null)"
conda activate paperless

jupyter serverextension enable jupytext
jupytext --set-formats ipynb,py KV_plots.py
