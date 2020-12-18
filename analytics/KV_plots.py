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
# Damn this file is a mess

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
os.environ['PAPERLESS_KV_DATA_DIR'] = "/home/wfloris/github/paperless/analytics"


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
def PlotOperationTimingOverTime(experiment):
    plt.figure(num=None, figsize=(15, 6), dpi=80, facecolor='w', edgecolor='k')
    print("Exoperiment: " + experiment)
    path = data_path + '/' + experiment + "/"
    for rank in ranks:

        print("--------------------------------------")
        print("Plots for ranks n=" + str(rank))
        plt.figure(num=None, figsize=(15, 6 * rank), dpi=80, facecolor='w', edgecolor='k')
        for i in range(min(show_first_n, rank)):
            i = i + 1

            gets = readOpTimes(path + 'ranks' + str(rank) + "/run1/get" + str(i) + ".txt")
            puts = readOpTimes(path + 'ranks' +str(rank) + "/run1/put" + str(i) + ".txt")

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
experiments = [
    'final/artificial_workload/paperless/ratio0']  # 'killPutTaskEuler/paperless', 'PE3', '8MB'] # 'nodePE2', 'nodePE3', '8MB']
ranks = [8, 16]
show_first_n = 8
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


def dataPath(experiment_name, ratio, rank_size, run_idx, db):
    return f"{data_path}/{experiment_name}/{db}/ratio{ratio}/ranks{rank_size}/run{run_idx}"



def GetLocalDataPoints(times, lables, local_rank):
    return times[lables == local_rank]
def GetRemoteDataPoints(times, lables, local_rank):
    return times[lables != local_rank]





def OpTimeForRun(experiment_name, run_idx, plot_data):
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


plot_data_1_core = pd.DataFrame(columns=['optime', 'optype', 'op_ratio' ,'rank_size', 'db'])
plot_data_1_core = OpTimeForRun('final/artificial_workload', 1, plot_data_1_core)
plot_data_1_core = OpTimeForRun('final/artificial_workload', 2, plot_data_1_core)

plot_data_2_core = pd.DataFrame(columns=['optime', 'optype', 'op_ratio' ,'rank_size', 'db'])
plot_data_2_core = OpTimeForRun('final/artificial_workload_2core', 1, plot_data_2_core)
plot_data_2_core = OpTimeForRun('final/artificial_workload_2core', 2, plot_data_2_core)


# +
def PlotSingleOperionTimes(plot_data):
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

                
PlotSingleOperionTimes(plot_data_1_core)
PlotSingleOperionTimes(plot_data_2_core)

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

# -
# # Relaxed vs sequential mode

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

data_path = os.environ['PAPERLESS_KV_DATA_DIR'] + "/data"
NANO_TO_SEC = 1000000000
# % of ops are updates

# I put it into class to not collide with Julia's code.
class RelSeqBench:
    def __init__(self, experiment_name, rank_sizes, n_runs, count):
        self.experiment_name = experiment_name
        self.rank_sizes = rank_sizes
        self.n_runs = n_runs
        self.count = count

    def dataPath(self, rank_size, run_idx, mode):
        return f"{data_path}/{self.experiment_name}/{mode}/ratio0/ranks{rank_size}/run{run_idx}"

    def throughputForRun(self, rank_size, run_idx, mode):
        p = self.dataPath(rank_size, run_idx, mode)
        total_put_t = 0
        for i in range(rank_size):
            with open(f'{p}/put_total{i}.txt') as f:
                total_put_t = total_put_t + int(f.read())
        return (rank_size * self.count) / (total_put_t / NANO_TO_SEC)

    # The throughput value is (# of ops)/(sum of per-op timings).
    def readThroughputData(self):
        throughputs = pd.DataFrame(columns=['throughput', 'rank_size', 'mode'])
        for mode in ("rel", "seq"):
            for _, rank_size in enumerate(self.rank_sizes):
                for i in range(1, self.n_runs + 1):
                    val = self.throughputForRun(rank_size=rank_size, run_idx=i, mode=mode)
                    throughputs = throughputs.append({'throughput':val, 'rank_size':rank_size, 'mode':mode}, ignore_index=True)
        return throughputs


    def plotThroughputs(self):
        throughput = self.readThroughputData()
        plt.title(f'Throughput in single rank per consistency')
        sns.boxplot(data=throughput, x='rank_size', y='throughput', hue='mode')
        # sns.swarmplot(data=throughputs[select], x='rank_size', y='throughput', hue='db')
        plt.yscale('log')
        plt.xlabel('Number of ranks')
        plt.ylabel('KPRS (kilo requests per second')
        plt.show()


bench = RelSeqBench('seq_vs_rel_2core/paperless', rank_sizes=[1,2, 4, 8, 12, 16, 24], n_runs=30, count=1000)
bench.plotThroughputs()





# # Vector Compare

# +
m_path = data_path + "/compare_vec/result_cmp_memcmp.txt"
v_path = data_path + "/compare_vec/result_cmp_vectorized.txt"

memcmp_cycles = pd.read_csv(m_path)
vector_cycles = pd.read_csv(v_path)

cycles = pd.concat([memcmp_cycles, vector_cycles])

def plotCompare(cycles):
    plt.title(f'Cycles < operator')
    sns.boxplot(data=cycles, x='key_len', y='cycles', hue='bench_name', showfliers=False)
    # sns.swarmplot(data=throughputs[select], x='rank_size', y='throughput', hue='db')
    
    plt.xlabel('key_length')
    plt.ylabel('cycles')
    plt.show()

plotCompare(cycles)



