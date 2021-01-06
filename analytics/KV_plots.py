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

# + [markdown] pycharm={}
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
import re


# Data Path
#os.environ['PAPERLESS_KV_DATA_DIR'] = "/home/julia/eth/dphpc/paperless/analytics"
#os.environ['PAPERLESS_KV_DATA_DIR'] = "/home/wfloris/github/paperless/analytics"


data_path = os.environ['PAPERLESS_KV_DATA_DIR'] + "/data"


# + [markdown] pycharm={}
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
rankPrefix = 'ranks'


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
            plt.title(f"Put rank {i+1}/{rank}")
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
            plt.title(f"Get rank {i+1}/{rank}")
        plt.show()


# + colab={"base_uri": "https://localhost:8080/", "height": 539} id="9vWxik0KpqO8" outputId="4b83a218-c7db-484c-ac74-14a304f019a0" pycharm={"is_executing": false}
experiments = ['cache_comp/hash/512ksize512vsize', 'cache_comp/tree/512ksize512vsize']  # 'killPutTaskEuler/paperless', 'PE3', '8MB'] # 'nodePE2', 'nodePE3', '8MB']
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



# +
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




def OpTimeForRunPerSize(experiment_name, rank_size, key_size, value_sizes, plot_data, db='', keysize_eq_value_size=False):
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



# +

value_sizes = [8, 32, 64, 256, 512, 1024, 2048, 4096]
key_size = 16
plot_data_1_core_per_size = pd.DataFrame(columns=['optime', 'value_size', 'op_ratio' ,'rank_size', 'db'])
plot_data_1_core_per_size = OpTimeForRunPerSize('size_compPE1', 16, key_size, value_sizes, plot_data_1_core_per_size)

plot_data_1_core_per_size_k = pd.DataFrame(columns=['optime', 'value_size', 'op_ratio' ,'rank_size', 'db'])
plot_data_1_core_per_size_k = OpTimeForRunPerSize('size_compPE1', 16, key_size, value_sizes, plot_data_1_core_per_size_k, keysize_eq_value_size=True)


# -

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
def PlotSingleOperionTimesPerRank(plot_data, optypes = ['localPut', 'localGet', 'remotePut', 'remoteGet'], lable='Optime for ', yLable = 'Operation time in nanoseconds'):
    for op_type in optypes:
        #for ratio in ratios:
        plt.yscale('log')
        plt.title( lable + op_type)
        select = (plot_data['optype'] == op_type) #& (plot_data['op_ratio'] == ratio)
        sns.boxplot(data=plot_data[select], x='rank_size', y='optime', hue='db', showfliers = False)
        #sns.swarmplot(data=plot_data[select], x='rank_size', y='optime', hue='db')
        #sns.violinplot(data=plot_data[select],  x='rank_size', y='optime', hue='db', split=True)
        plt.yscale('log')
        plt.xlabel('Number of ranks')
        plt.ylabel(yLable)
        plt.show()



# -
PlotSingleOperionTimesPerRank(plot_data_1_core_per_rank)
PlotSingleOperionTimesPerRank(plot_data_2_core_per_rank)


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

    opThroughputTotal = sum(map(calculateThroughput, allGetTimes)) + sum(map(calculateThroughput, allUpdateTimes))
    opThroughputPerRank = calculateThroughput(opTimes)
    return opThroughputPerRank, opThroughputTotal

def readThroughputData(experiment_name):
    throughputs = pd.DataFrame(columns=['throughput', 'throughput_per_rank', 'rank_size', 'op_ratio', 'KV store'])
    for k, ratio in enumerate(ratios):
        for j, rank_size in enumerate(rank_sizes):
            for i in range(1, n_runs + 1):
                for db in ('paperless', 'papyrus'):
                    valPerRank, valTotal = throughputForRun(experiment_name, ratio=ratio, rank_size=rank_size, run_idx=i, db=db)
                    throughputs = throughputs.append({
                        'throughput': valTotal,
                        'throughput_per_rank': valPerRank,
                        'rank_size': rank_size,
                        'op_ratio': ratio,
                        'KV store': db
                    }, ignore_index=True)
    return throughputs
# The throughput value is (# of ops)/(sum of per-op timings).



# + pycharm={"metadata": false, "name": "#%%\n", "is_executing": false}

def plotThroughputs(throughputs, name, per_rank=False):
    for k, ratio in enumerate(ratios):
        select = throughputs['op_ratio'] == ratio
        val_column = 'throughput_per_rank' if per_rank else 'throughput'
        if per_rank:
            title = f'Micro-averaged throughput (per rank) with {100-ratio}% gets and {ratio}% updates'
        else:
            title = f'Total throughput with {100-ratio}% gets and {ratio}% updates'
        plt.title(title)
        sns.boxplot(data=throughputs[select], x='rank_size', y=val_column, hue='KV store')
        # sns.swarmplot(data=throughputs[select], x='rank_size', y='throughput', hue='db')
        plt.yscale('log')
        plt.xlabel('Number of ranks')
        plt.ylabel('KPRS (kilo requests per second)')
        plt.savefig(f"./plots/{name}_{ratio}_{'per_rank' if per_rank else 'total'}.png", dpi=600)
        plt.show()


# + pycharm={"is_executing": false, "metadata": false, "name": "#%%#%%\n"}

single_core_data = readThroughputData('final/artificial_workload')
two_core_data = readThroughputData('final/artificial_workload_2core')

plotThroughputs(single_core_data)
plotThroughputs(two_core_data)

# -
# # Throughput plots II
#

# +

NANO_TO_SEC = 1000000000

def GetThroughputData(experiment):
    with open(data_path + experiment, 'r') as f:
        lines = f.readlines()

    pairs = list(zip(lines[0::2], lines[1::2]))

    reg = re.compile("mpirun --map-by node:PE=2 -np ([0-9]+) ./build/throughput_([a-zA-z]+) [0-9]+ [0-9]+ ([0-9]+) ([0-9]+) [0-9a-zA-z/]+")


    plot_data = pd.DataFrame(columns=['optime', 'optype', 'op_ratio' ,'rank_size', 'db'] )
    for a, b in pairs:
        reg_res = reg.match(a)
        rank_size = int(reg_res.group(1))
        db = reg_res.group(2)
        count = int(reg_res.group(3))
        ratio = int(reg_res.group(4))

        nums = b.split(" ")
        put_through = count / int(nums[0]) * NANO_TO_SEC
        get_through = count / int(nums[1]) * NANO_TO_SEC 

        plot_data = plot_data.append({'optime': put_through,
                                                   'optype' : "put",
                                                   'op_ratio': ratio,
                                                   'rank_size':rank_size,
                                                   'db': db}, ignore_index=True)

        plot_data = plot_data.append({'optime': get_through,
                                                   'optype' : "update/get",
                                                   'op_ratio': ratio,
                                                   'rank_size':rank_size,
                                                   'db': db}, ignore_index=True)
    return plot_data



# +

experiments = ['/throughput/5PercentPutsLessInserts.txt','/throughput/out4.txt','/throughput/5PercentPuts.txt']

for experiment in experiments:
    PlotSingleOperionTimesPerRank(GetThroughputData(experiment), ["update/get", "put"],lable = experiment + ', Throughput for ', yLable= 'Operations per Second')
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





# # Hash vs Tree Cache
#

# +
experiment = 'cache_comp'
caches = ['hash', 'tree']
sizes = [8,32,512,1024,4096]
ranks = [4,1]

for rank in ranks:
    plot_data = pd.DataFrame(columns=['optime', 'value_size', 'op_ratio' ,'rank_size', 'db'])
    plot_data = OpTimeForRunPerSize(experiment + '/hash', rank, 0, sizes, plot_data, 'hash', keysize_eq_value_size=True)
    plot_data = OpTimeForRunPerSize(experiment + '/tree', rank, 0, sizes, plot_data, 'tree', keysize_eq_value_size=True)
    PlotSingleOperionTimesPerSize(plot_data, rank, 'Size of value im Bytes')


# -

# # Vector Compare

# +
m_path = data_path + "/compare_vec/result_cmp_memcmp.txt"
v_path = data_path + "/compare_vec/result_cmp_vectorized.txt"

memcmp_cycles = pd.read_csv(m_path)
vector_cycles = pd.read_csv(v_path)

cycles = pd.concat([memcmp_cycles, vector_cycles])

def plotCompare(cycles):
    plt.title(f'Cycles operator< (avx2 vs memcmp)')
    sns.boxplot(data=cycles, x='key_len', y='cycles', hue='bench_name', showfliers=False)
    # sns.swarmplot(data=throughputs[select], x='rank_size', y='throughput', hue='db')

    plt.xlabel('key_length')
    plt.ylabel('cycles')
    plt.show()

plotCompare(cycles)


