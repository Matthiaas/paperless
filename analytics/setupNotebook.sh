#!/usr/bin/env bash
conda env create -f environment.yml
jupyter serverextension enable jupytext
jupytext --set-formats ipynb,py KV_plots.py
