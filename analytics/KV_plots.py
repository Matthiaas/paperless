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
import warnings
# %matplotlib inline

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os


# + id="OFvbZi__9c_j" pycharm={"is_executing": false}
def readVector(file_name, ignore_empty=False):
    if ignore_empty:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore") 
            matrix = np.loadtxt(file_name)
    else:
        matrix = np.loadtxt(file_name)
    if len(matrix) == 0 and ignore_empty:
        return np.empty(0), np.empty(0)
    # return values, colors
    return matrix[:, 0], matrix[:, 1]

# + [markdown] id="cALaPDztnH_X" pycharm={}
# # Paperless and Papyrus plots
#

# + id="BJa3a46qnhdl" pycharm={"is_executing": false}
experiments = [
    'IprobeExperimentEuler/paperless']  # 'killPutTaskEuler/paperless', 'PE3', '8MB'] # 'nodePE2', 'nodePE3', '8MB']
ranks = [8, 16, 32]
show_first_n = 1
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
        plt.figure(num=None, figsize=(15, 6 * rank), dpi=80, facecolor='w', edgecolor='k')
        for i in range(min(show_first_n, rank)):
            i = i + 1

            gets = readVector(path + str(rank) + "/get" + str(i) + ".txt")
            puts = readVector(path + str(rank) + "/put" + str(i) + ".txt")

            # print(f"Rank owners for PUT on {i}/{rank}: {np.unique(puts[1])}")
            # print(f"Rank owners for GET on {i}/{rank}: {np.unique(gets[1])}")

            ax = plt.subplot(rank, 2, 2 * i + 1)
            plt.yscale('log')
            scatter = plt.scatter(list(range(0, len(puts[0]))), puts[0], 0.5, puts[1])
            legend1 = ax.legend(*scatter.legend_elements(),
                                loc="upper right", title="Key owner")
            ax.add_artist(legend1)
            plt.title(f"Put rank {i}/{rank}")
            plt.ylabel("time")
            plt.xlabel("nth operation")

            ax = plt.subplot(rank, 2, 2 * i + 2)
            plt.yscale('log')
            scatter = plt.scatter(list(range(0, len(gets[0]))), gets[0], 0.5, gets[1])
            legend1 = ax.legend(*scatter.legend_elements(),
                                loc="upper right", title="Key owner")
            ax.add_artist(legend1)
            plt.ylabel("time")
            plt.xlabel("nth operation")
            plt.title(f"Get rank {i}/{rank}")
        plt.show()


# + colab={"base_uri": "https://localhost:8080/", "height": 1000} id="pdmV9lquni-5" outputId="03cbd4a8-cf53-4e59-cdcc-b4d39a791bdf" pycharm={"is_executing": false, "name": "#%% \n"}

# Artificial workload benchmark plots
os.environ['PAPERLESS_KV_DATA_DIR'] = "/home/julia/eth/dphpc/paperless/analytics"
path = os.environ['PAPERLESS_KV_DATA_DIR'] + "/data"

# % of ops are updates
ratios = [0, 50, 5]
rank_sizes = [1, 2, 4, 8, 12, 16]
n_runs = 10

NANO_TO_SEC = 1000000000


def dataPath(experiment_name, ratio, rank_size, run_idx, db):
    return f"{path}/{experiment_name}/{db}/ratio{ratio}/ranks{rank_size}/run{run_idx}"


def calculateThroughput(vector):
    if len(vector):
        return len(vector) / sum(vector) * NANO_TO_SEC
    else:
        return 0


# We consider only get and update operations (not puts).
def throughputForRun(experiment_name, ratio, rank_size, run_idx, db):
    p = dataPath(experiment_name, ratio, rank_size, run_idx, db)
    allGetTimes = []
    allUpdateTimes = []
    for i in range(rank_size):
        getPath = f"{p}/get{i}.txt"
        getTimes, _ = readVector(getPath)
        updatePath = f"{p}/update{i}.txt"
        updateTimes, _ = readVector(updatePath, ignore_empty=True)
        allGetTimes.append(getTimes)
        allUpdateTimes.append(updateTimes)
    opTimes = np.concatenate(allGetTimes + allUpdateTimes, axis=0)
    
    opThroughputLikeInPaper = sum(map(calculateThroughput, allGetTimes)) + sum(map(calculateThroughput, allUpdateTimes))
    opThroughput = calculateThroughput(opTimes)
    return opThroughput, opThroughputLikeInPaper

def readThroughputData(experiment_name):    
    throughputs = pd.DataFrame(columns=['throughput', 'rank_size', 'op_ratio', 'db'])
    for k, ratio in enumerate(ratios):
        for j, rank_size in enumerate(rank_sizes):
            for i in range(1, n_runs + 1):
                for db in ('paperless', 'papyrus'):
                    val, _ = throughputForRun(experiment_name, ratio=ratio, rank_size=rank_size, run_idx=i, db=db)
                    throughputs = throughputs.append({'throughput':val, 'rank_size':rank_size, 'op_ratio':ratio, 'db':db}, ignore_index=True)
    return throughputs
# The throughput value is (# of ops)/(sum of per-op timings).



# + pycharm={"metadata": false, "name": "#%%\n", "is_executing": false}

def plotThroughputs(throughputs):
    for k, ratio in enumerate(ratios):
        select = throughputs['op_ratio'] == ratio
        plt.title(f'Performance with {100-ratio}% gets and {ratio}% updates')
        sns.boxplot(data=throughputs[select], x='rank_size', y='throughput', hue='db')
        # sns.swarmplot(data=throughputs[select], x='rank_size', y='throughput', hue='db')
        plt.yscale('log')
        plt.xlabel('Number of ranks')
        plt.ylabel('KPRS (kilo requests per second')
        plt.show()

# + pycharm={"metadata": false, "name": "#%%#%%\n", "is_executing": false}


single_core_data = readThroughputData('final/artificial_workload')
two_core_data = readThroughputData('final/artificial_workload_2core')

plotThroughputs(single_core_data)
plotThroughputs(two_core_data)

# + id="5t-v0JFD_Js9" pycharm={"is_executing": false}



# + pycharm={"metadata": false, "name": "#%%\n"}


