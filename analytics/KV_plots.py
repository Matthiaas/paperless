# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
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
experiments = ['IprobeExperimentEuler/paperless'] #'killPutTaskEuler/paperless', 'PE3', '8MB'] # 'nodePE2', 'nodePE3', '8MB']
ranks = [16,32]
show_first_n = 32
# experiment = 'localRun'
# ranks = [8]
# experiment = 'local-julia'
# ranks = [8]
# + pycharm={"is_executing": false}



# + colab={"base_uri": "https://localhost:8080/", "height": 539} id="9vWxik0KpqO8" outputId="4b83a218-c7db-484c-ac74-14a304f019a0" pycharm={"is_executing": false}



for experiment in experiments:
    plt.figure(num=None, figsize=(15, 6), dpi=80, facecolor='w', edgecolor='k')
    print("Exoperiment: " + experiment)
    path = os.environ['PAPERLESS_KV_DATA_DIR'] + "/data/" + experiment + "/"
    for rank in ranks:


        print("--------------------------------------")
        print("Plots for ranks n=" + str(rank))
        plt.figure(num=None, figsize=(15, 6* rank), dpi=80, facecolor='w', edgecolor='k')
        for i in range(min(show_first_n,rank)):
            gets = readVector(path + str(rank) + "/get" + str(i) + ".txt")
            puts = readVector(path + str(rank) + "/put" + str(i) + ".txt")

            #print(f"Rank owners for PUT on {i}/{rank}: {np.unique(puts[1])}")
            #print(f"Rank owners for GET on {i}/{rank}: {np.unique(gets[1])}")

            ax = plt.subplot(rank, 2, 2*i +1)
            plt.yscale('log')
            scatter = plt.scatter(list(range(0, len(puts[0]))), puts[0], 0.5, puts[1])
            legend1 = ax.legend(*scatter.legend_elements(),
                            loc="upper right", title="Key owner")
            ax.add_artist(legend1)
            plt.title(f"Put rank {i}/{rank}")
            plt.ylabel("time")
            plt.xlabel("nth operation")

            ax = plt.subplot(rank, 2, 2*i + 2)
            plt.yscale('log')
            scatter = plt.scatter(list(range(0, len(gets[0]))), gets[0], 0.5, gets[1])
            legend1 = ax.legend(*scatter.legend_elements(),
                            loc="upper right", title="Key owner")
            ax.add_artist(legend1)
            plt.ylabel("time")
            plt.xlabel("nth operation")
            plt.title(f"Get rank {i}/{rank}")
        plt.show()


# + colab={"base_uri": "https://localhost:8080/", "height": 1000} id="pdmV9lquni-5" outputId="03cbd4a8-cf53-4e59-cdcc-b4d39a791bdf" pycharm={"is_executing": false}



# + id="5t-v0JFD_Js9" pycharm={}

