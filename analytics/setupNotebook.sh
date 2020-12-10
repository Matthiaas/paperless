#!/usr/bin/env bash
sudo apt install jupyter -y
sudo pip3 install jupytext
jupyter serverextension enable jupytext
jupytext --set-formats ipynb,py KV_plots.py
