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

# # Paperless and Papyrus plots
#

# + id="zeWCU8XvnRBH" pycharm={"is_executing": false}
import warnings
# %matplotlib inline

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os


# Data Path
#os.environ['PAPERLESS_KV_DATA_DIR'] = "/home/julia/eth/dphpc/paperless/analytics"
data_path = os.environ['PAPERLESS_KV_DATA_DIR'] + "/data"


# -

# ## FileOperations

# + id="OFvbZi__9c_j" pycharm={"is_executing": false}
def readOpTimes(file_name, ignore_empty=False):
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


# -

# ## Single Operation Plots
# ### Operations Over Time

# + pycharm={"is_executing": false}
rankPostfix = '' # '/run1'
rankPrefix = '' # 'ranks'


def PlotOperationTimingOverTime(experiment):
    plt.figure(num=None, figsize=(15, 6), dpi=80, facecolor='w', edgecolor='k')
    print("Exoperiment: " + experiment)
    path = data_path + '/' + experiment + "/"
    for rank in ranks:

        print("--------------------------------------")
        print("Plots for ranks n=" + str(rank))
        plt.figure(num=None, figsize=(15, 6 * rank), dpi=80, facecolor='w', edgecolor='k')
        for i in range(min(show_first_n, rank)):
            gets = readOpTimes(path + rankPrefix + str(rank) + rankPostfix + "/get" + str(i) + ".txt")
            puts = readOpTimes(path + rankPrefix +str(rank) + rankPostfix + "/put" + str(i) + ".txt")

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


# + colab={"base_uri": "https://localhost:8080/", "height": 539} id="9vWxik0KpqO8" outputId="4b83a218-c7db-484c-ac74-14a304f019a0" pycharm={"is_executing": false}
experiments = ['8MB', 'killPutTaskEuler/paperless', 'paperlessMessagePE2/paperless']  # 'killPutTaskEuler/paperless', 'PE3', '8MB'] # 'nodePE2', 'nodePE3', '8MB']
ranks = [8]
show_first_n = 1
# experiment = 'localRun'
# ranks = [8]
# experiment = 'local-julia'
# ranks = [8]
for experiment in experiments:
    PlotOperationTimingOverTime(experiment)
    
# -


# ### Single Opearion Time Distribution

# +
experiment = 'final/artificial_workload'
ratios = [0, 50, 5]
rank_sizes = [1, 2, 4, 8, 12, 16]
dbs = ['paperless', 'papyrus']



def GetLocalDataPoints(times, lables, local_rank):
    return times[lables == local_rank]
def GetRemoteDataPoints(times, lables, local_rank):
    return times[lables != local_rank]



def OpTimeForRunPerRank(experiment_name, run_idx, plot_data):
    
    def dataPath(experiment_name, ratio, rank_size, run_idx, db):
        return f"{data_path}/{experiment_name}/{db}/ratio{ratio}/ranks{rank_size}/run{run_idx}"


    for db in dbs:
        for ratio in ratios:
            for rank_size in rank_sizes:
                p = dataPath(experiment_name, ratio, rank_size, run_idx, db)
                for i in range(rank_size):
                    getPath = f"{p}/get{i}.txt"
                    getTimes, getLabels = readOpTimes(getPath)
                    putPath = f"{p}/put{i}.txt"
                    putTimes, putLables = readOpTimes(putPath)

                    localPuts = GetLocalDataPoints(putTimes, putLables, i)
                    localGets = GetLocalDataPoints(getTimes, getLabels, i)

                    remotePuts = GetRemoteDataPoints(putTimes, putLables, i)
                    remoteGets = GetRemoteDataPoints(getTimes, getLabels, i)

                    plot_data = plot_data.append(pd.DataFrame({'optime': localPuts, 
                                              'optype' : np.full(len(localPuts), "localPut"),
                                              'op_ratio': np.full(len(localPuts), ratio),
                                              'rank_size':np.full(len(localPuts), rank_size), 
                                              'db':np.full(len(localPuts), db)}))
                    plot_data = plot_data.append(pd.DataFrame({'optime': localGets, 
                                              'optype' : np.full(len(localGets), "localGet"),
                                              'op_ratio': np.full(len(localGets), ratio),
                                              'rank_size':np.full(len(localGets), rank_size), 
                                              'db':np.full(len(localGets), db)}))

                    plot_data = plot_data.append(pd.DataFrame({'optime': remotePuts, 
                                              'optype' : np.full(len(remotePuts), "remotePut"),
                                              'op_ratio': np.full(len(remotePuts), ratio),
                                              'rank_size':np.full(len(remotePuts), rank_size), 
                                              'db':np.full(len(remotePuts), db)}))
                    plot_data =  plot_data.append(pd.DataFrame({'optime': remoteGets, 
                                              'optype' : np.full(len(remoteGets), "remoteGet"),
                                              'op_ratio': np.full(len(remoteGets), ratio),
                                              'rank_size':np.full(len(remoteGets), rank_size), 
                                              'db':np.full(len(remoteGets), db)}))
                          
    return plot_data

    



plot_data_1_core_per_rank = pd.DataFrame(columns=['optime', 'optype', 'op_ratio' ,'rank_size', 'db'])
plot_data_1_core_per_rank = OpTimeForRunPerRank('final/artificial_workload', 1, plot_data_1_core_per_rank)
plot_data_1_core_per_rank = OpTimeForRunPerRank('final/artificial_workload', 2, plot_data_1_core_per_rank)

plot_data_2_core_per_rank = pd.DataFrame(columns=['optime', 'optype', 'op_ratio' ,'rank_size', 'db'])
plot_data_2_core_per_rank = OpTimeForRunPerRank('final/artificial_workload_2core', 1, plot_data_2_core_per_rank)
plot_data_2_core_per_rank = OpTimeForRunPerRank('final/artificial_workload_2core', 2, plot_data_2_core_per_rank)




#plot_data_2_core_per_size = pd.DataFrame(columns=['optime', 'value_size', 'op_ratio' ,'rank_size', 'db'])
#plot_data_2_core_per_size = OpTimeForRunPerRank('final/artificial_workload_2core', 1, plot_data_2_core_per_size)
#plot_data_2_core_per_size = OpTimeForRunPerRank('final/artificial_workload_2core', 2, plot_data_2_core_per_size)


# +

value_sizes = [8, 32, 64, 256, 512, 1024, 2048, 4096]
key_size = 16


def OpTimeForRunPerSize(experiment_name, rank_size, key_size, plot_data, keysize_eq_value_size=False):
    k_size = key_size
    for value_size in value_sizes:
        if keysize_eq_value_size:
            k_size = value_size
        print(value_size)
        for i in range(rank_size):
            
            p = f"{data_path}/{experiment_name}/{k_size}ksize{value_size}vsize/ranks{rank_size}"
            getPath = f"{p}/get{i}.txt"
            getTimes, getLabels = readOpTimes(getPath)
            putPath = f"{p}/put{i}.txt"
            putTimes, putLables = readOpTimes(putPath)

            localPuts = GetLocalDataPoints(putTimes, putLables, i)
            localGets = GetLocalDataPoints(getTimes, getLabels, i)

            remotePuts = GetRemoteDataPoints(putTimes, putLables, i)
            remoteGets = GetRemoteDataPoints(getTimes, getLabels, i)

            plot_data = plot_data.append(pd.DataFrame({'optime': localPuts, 
                                      'optype' : np.full(len(localPuts), "localPut"),
                                      'value_size': np.full(len(localPuts), value_size),
                                      'rank_size':np.full(len(localPuts), rank_size), 
                                      'db':np.full(len(localPuts), db)}))
            plot_data = plot_data.append(pd.DataFrame({'optime': localGets, 
                                      'optype' : np.full(len(localGets), "localGet"),
                                      'value_size': np.full(len(localGets), value_size),
                                      'rank_size':np.full(len(localGets), rank_size), 
                                      'db':np.full(len(localGets), db)}))

            plot_data = plot_data.append(pd.DataFrame({'optime': remotePuts, 
                                      'optype' : np.full(len(remotePuts), "remotePut"),
                                      'value_size': np.full(len(remotePuts), value_size),
                                      'rank_size':np.full(len(remotePuts), rank_size), 
                                      'db':np.full(len(remotePuts), db)}))
            plot_data =  plot_data.append(pd.DataFrame({'optime': remoteGets, 
                                      'optype' : np.full(len(remoteGets), "remoteGet"),
                                      'value_size': np.full(len(remoteGets), value_size),
                                      'rank_size':np.full(len(remoteGets), rank_size), 
                                      'db':np.full(len(remoteGets), db)}))

    return plot_data

plot_data_1_core_per_size = pd.DataFrame(columns=['optime', 'value_size', 'op_ratio' ,'rank_size', 'db'])
plot_data_1_core_per_size = OpTimeForRunPerSize('size_compPE1', 16, key_size, plot_data_1_core_per_size)

plot_data_1_core_per_size_k = pd.DataFrame(columns=['optime', 'value_size', 'op_ratio' ,'rank_size', 'db'])
plot_data_1_core_per_size_k = OpTimeForRunPerSize('size_compPE1', 16, key_size, plot_data_1_core_per_size, True)


# +
def PlotSingleOperionTimesPerSize(plot_data, rank_size, lable):
    for op_type in ['localPut', 'localGet', 'remotePut', 'remoteGet']:
        #for ratio in ratios:
        plt.yscale('log')
        plt.title(f'Optime for {op_type} with {rank_size} ranks')
        select = (plot_data['optype'] == op_type) #& (plot_data['op_ratio'] == ratio)
        sns.boxplot(data=plot_data[select], x='value_size', y='optime', hue='db', showfliers = False)
        #sns.swarmplot(data=plot_data[select], x='rank_size', y='optime', hue='db')
        #sns.violinplot(data=plot_data[select],  x='rank_size', y='optime', hue='db', split=True)
        plt.yscale('log')
        plt.xlabel(lable)
        plt.ylabel('Operation time in nanoseconds')
        plt.ylim((100,30000))
        plt.show()
        
PlotSingleOperionTimesPerSize(plot_data_1_core_per_size, 16, 'Size of value im Bytes')
        
PlotSingleOperionTimesPerSize(plot_data_1_core_per_size, 16, 'Size of value=key im Bytes')



# +
def PlotSingleOperionTimesPerRank(plot_data):
    for op_type in ['localPut', 'localGet', 'remotePut', 'remoteGet']:
        #for ratio in ratios:
        plt.yscale('log')
        plt.title(f'Optime for {op_type}')
        select = (plot_data['optype'] == op_type) #& (plot_data['op_ratio'] == ratio)
        sns.boxplot(data=plot_data[select], x='rank_size', y='optime', hue='db', showfliers = False)
        #sns.swarmplot(data=plot_data[select], x='rank_size', y='optime', hue='db')
        #sns.violinplot(data=plot_data[select],  x='rank_size', y='optime', hue='db', split=True)
        plt.yscale('log')
        plt.xlabel('Number of ranks')
        plt.ylabel('Operation time in nanoseconds')
        plt.show()

                
PlotSingleOperionTimesPerRank(plot_data_1_core_per_rank)
PlotSingleOperionTimesPerRank(plot_data_2_core_per_rank)
# -



# # Throughput plots

# + colab={"base_uri": "https://localhost:8080/", "height": 1000} id="pdmV9lquni-5" outputId="03cbd4a8-cf53-4e59-cdcc-b4d39a791bdf" pycharm={"is_executing": false, "name": "#%% \n"}

# Artificial workload benchmark plots


# % of ops are updates
ratios = [0, 50, 5]
rank_sizes = [1, 2, 4, 8, 12, 16]
n_runs = 10

NANO_TO_SEC = 1000000000


def dataPath(experiment_name, ratio, rank_size, run_idx, db):
    return f"{data_path}/{experiment_name}/{db}/ratio{ratio}/ranks{rank_size}/run{run_idx}"


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
        getTimes, _ = readOpTimes(getPath)
        updatePath = f"{p}/update{i}.txt"
        updateTimes, _ = readOpTimes(updatePath, ignore_empty=True)
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


