# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + id="zeWCU8XvnRBH" pycharm={"is_executing": false}
# %matplotlib inline

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

import os


# + id="OFvbZi__9c_j" pycharm={"is_executing": false}
def readVector(file_name):
    matrix = np.loadtxt(file_name)
    
    # return values, colors
    return matrix[:,0], matrix[:,1]

# + [markdown] id="cALaPDztnH_X" pycharm={}
# # Paperless and Papyrus plots
#

# + id="BJa3a46qnhdl" pycharm={"is_executing": false}
experiment = '8MB'
ranks = [1, 2, 4, 8]
# experiment = 'localRun'
# ranks = [8]
# experiment = 'local-julia'
# ranks = [8]


path = "/home/julia/eth/dphpc/paperless/analytics/data/" + experiment + "/"
print(path)

# + pycharm={"is_executing": false}



# + colab={"base_uri": "https://localhost:8080/", "height": 539} id="9vWxik0KpqO8" outputId="4b83a218-c7db-484c-ac74-14a304f019a0" pycharm={"is_executing": false}

plt.figure(num=None, figsize=(15, 6), dpi=80, facecolor='w', edgecolor='k')

for rank in ranks:
  gets = []
  puts = []

  for i in range(8):
    print("--------------------------------------")
  print("Plots for ranks n=" + str(rank))
  plt.figure(num=None, figsize=(15, 6* rank), dpi=80, facecolor='w', edgecolor='k')
  for i in range(rank):
        
    
    gets.append(readVector(path + str(rank) + "/get" + str(i) + ".txt"))
    puts.append(readVector(path + str(rank) + "/put" + str(i) + ".txt"))
    
    print(f"Rank owners for PUT on {i}/{rank}: {np.unique(puts[i][1])}")
    print(f"Rank owners for GET on {i}/{rank}: {np.unique(gets[i][1])}")
    
    ax = plt.subplot(rank, 2, 2*i +1)
    plt.yscale('log')
    scatter = plt.scatter(list(range(0, len(puts[i][0]))), puts[i][0], 0.5, puts[i][1])
    legend1 = ax.legend(*scatter.legend_elements(),
                    loc="upper right", title="Key owner")
    ax.add_artist(legend1)
    plt.title(f"Put rank {i}/{rank}")
    plt.ylabel("time")
    plt.xlabel("nth operation")
    
    ax = plt.subplot(rank, 2, 2*i + 2)
    plt.yscale('log')
    scatter = plt.scatter(list(range(0, len(gets[i][0]))), gets[i][0], 0.5, gets[i][1])
    legend1 = ax.legend(*scatter.legend_elements(),
                    loc="upper right", title="Key owner")
    ax.add_artist(legend1)
    plt.ylabel("time")
    plt.xlabel("nth operation")
    plt.title(f"Get rank {i}/{rank}")
  plt.show()


# + colab={"base_uri": "https://localhost:8080/", "height": 1000} id="pdmV9lquni-5" outputId="03cbd4a8-cf53-4e59-cdcc-b4d39a791bdf" pycharm={"is_executing": false}



# + id="5t-v0JFD_Js9" pycharm={}

