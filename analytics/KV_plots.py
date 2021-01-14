# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.9.1
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
import inspect
import collections

# Data Path
#os.environ['PAPERLESS_KV_DATA_DIR'] = "/home/julia/eth/dphpc/paperless/analytics"
#os.environ['PAPERLESS_KV_DATA_DIR'] = "/home/wfloris/github/paperless/analytics"


data_path = os.environ['PAPERLESS_KV_DATA_DIR'] + "/data/paperless-data"
plot_dir_path = os.environ['PAPERLESS_KV_DATA_DIR'] + "/plots/"


save_plots = False


# This method will create a directory for you (dependent on the function name)
# Just enter the file_name.
def plotToSVG(file_name):
    dir_path = plot_dir_path + inspect.stack()[1][3]
    os.makedirs(dir_path, exist_ok=True)
    plt.savefig(dir_path + "/" + file_name + ".svg")


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
rankPostfix = '/run1'' # '
rankPrefix = 'ranks'

def dataPath(experiment_name, ratio, rank_size, run_idx, db):
        return f"{data_path}/{experiment_name}/{db}/ratio{ratio}/ranks{rank_size}/run{run_idx}"


def PlotOperationTimingOverTime(experiment):
    plt.figure(num=None, figsize=(15, 6), dpi=80, facecolor='w', edgecolor='k')
    print("Exoperiment: " + experiment)
    for rank in ranks:

        print("--------------------------------------")
        print("Plots for ranks n=" + str(rank))
        plt.figure(num=None, figsize=(15, 6 * rank), dpi=80, facecolor='w', edgecolor='k')
        for i in range(min(show_first_n, rank)):
            gets = readOpTimes(dataPath(experiment, 0, rank, 1, "paperless") + "/get" + str(i) + ".txt" )
            puts = readOpTimes(dataPath(experiment, 0, rank, 1, "paperless")  + "/put" + str(i) + ".txt")

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
experiments = ['opt_time_report_n_host']  # 'killPutTaskEuler/paperless', 'PE3', '8MB'] # 'nodePE2', 'nodePE3', '8MB']
ranks = [16]
show_first_n = 4
# experiment = 'localRun'
# ranks = [8]
# experiment = 'local-julia'
# ranks = [8]
for experiment in experiments:
    PlotOperationTimingOverTime(experiment)

# -


# ### Single Opearion Time Distribution

# +
ratios = [0, 50, 5]
rank_sizes = [1, 2, 4, 8, 16, 24]
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
#plot_data_1_core_per_rank = pd.DataFrame(columns=['optime', 'optype', 'op_ratio' ,'rank_size', 'db'])
#plot_data_1_core_per_rank = OpTimeForRunPerRank('artificial_workload_1core_final', 1, plot_data_1_core_per_rank)
#plot_data_1_core_per_rank = OpTimeForRunPerRank('artificial_workload_1core_final', 2, plot_data_1_core_per_rank)

plot_data_2_core_per_rank = pd.DataFrame(columns=['optime', 'optype', 'op_ratio' ,'rank_size', 'db'])
plot_data_2_core_per_rank = OpTimeForRunPerRank('opt_time_report_one_host', 1, plot_data_2_core_per_rank)
plot_data_2_core_per_rank = OpTimeForRunPerRank('opt_time_report_one_host', 2, plot_data_2_core_per_rank)


# +

plot_data_2_core_n_host_per_rank = pd.DataFrame(columns=['optime', 'optype', 'op_ratio' ,'rank_size', 'db'])
plot_data_2_core_n_host_per_rank = OpTimeForRunPerRank('opt_time_report_n_host', 1, plot_data_2_core_n_host_per_rank)
plot_data_2_core_n_host_per_rank = OpTimeForRunPerRank('opt_time_report_n_host', 2, plot_data_2_core_n_host_per_rank)


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



PlotSingleOperionTimesPerSize(plot_data_1_core_per_size, 16, 'Size of value im Bytes')
PlotSingleOperionTimesPerSize(plot_data_1_core_per_size, 16, 'Size of value=key im Bytes')


def PlotSingleOperionTimesPerRank(plot_data, optypes = ['localPut', 'localGet', 'remotePut', 'remoteGet'], lable='Optime for ', yLable = 'Operation time in nanoseconds'):
    for op_type in optypes:
        for ratio in ratios:
            plt.yscale('log')
            plt.title( lable + op_type +  ", ratio: " + str(ratio))
            select = (plot_data['optype'] == op_type) & (plot_data['op_ratio'] == ratio)
            sns.boxplot(data=plot_data[select], x='rank_size', y='optime', hue='db', showfliers = False)
            #sns.swarmplot(data=plot_data[select], x='rank_size', y='optime', hue='db')
            #sns.violinplot(data=plot_data[select],  x='rank_size', y='optime', hue='db', split=True)
            plt.yscale('log')
            plt.xlabel('Number of ranks')
            plt.ylabel(yLable)
            if save_plots:
                plotToSVG(op_type)
            else:
                plt.show()




# +
def PlotDist(data, name,lable='Optime for ', yLable = 'Operation time in nanoseconds'):         
    fig, ax = plt.subplots()
    #sns.swarmplot(data=plot_data[select], x='rank_size', y='optime', hue='db')
    #sns.violinplot(data=plot_data[select],  x='rank_size', y='optime', hue='db', split=True)
    print(lable + name)
    # remove the next two lines to remove te cdf
    ax2 = ax.twinx()
    sns.ecdfplot(data=data, 
                 x='optime', 
                 hue='db', 
                 log_scale=(False,False),
                ax = ax2)
    sns.histplot(data=data,
                 x='optime',
                 hue='db',
                 log_scale=(True,True),
                 binwidth=0.05,
                 ax=ax, 
                 element="step")
    ax.set(xlabel='optime in ns')
    if save_plots:
        plotToSVG(name)
    else:
        plt.show()

def PlotSingleOperionTimesDist(plot_data, rank=16, optypes = ['localPut', 'localGet', 'remotePut', 'remoteGet'], lable='Optime for ', yLable = 'Operation time in nanoseconds'):
    for op_type in optypes:
        #for ratio in ratios:
        
        select = (plot_data['optype'] == op_type) & (plot_data['rank_size'] == rank)
        PlotDist(plot_data[select], op_type, lable=lable, yLable=yLable)

            
def PlotSingleOperionTimesDistOnlyLocaLRemote(plot_data, rank=16, lable='Optime for ', yLable = 'Operation time in nanoseconds'):
    
    get_select =  (((plot_data['optype'] == 'localPut') | (plot_data['optype'] == 'remotePut')  ) 
                            & (plot_data['rank_size'] == rank))
    put_select = (((plot_data['optype'] == 'localGet') | (plot_data['optype'] == 'remoteGet')  ) 
                            & (plot_data['rank_size'] == rank))
    PlotDist(plot_data[get_select], "get", lable=lable, yLable=yLable)
    PlotDist(plot_data[put_select], "put", lable=lable, yLable=yLable)
    


# -

PlotSingleOperionTimesDistOnlyLocaLRemote(plot_data_2_core_n_host_per_rank)


PlotSingleOperionTimesDistOnlyLocaLRemote(plot_data_2_core_per_rank)

# +
#PlotSingleOperionTimesPerRank(plot_data_2_core_n_host_per_rank)
#
# -




# # Throughput plots

# + colab={"base_uri": "https://localhost:8080/", "height": 1000} id="pdmV9lquni-5" outputId="03cbd4a8-cf53-4e59-cdcc-b4d39a791bdf" pycharm={"is_executing": false, "name": "#%% \n"}
"""
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
"""
0

# + pycharm={"metadata": false, "name": "#%%\n", "is_executing": false}
"""
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
"""
0

# + pycharm={"is_executing": false, "metadata": false, "name": "#%%#%%\n"}
"""
single_core_data = readThroughputData('final/artificial_workload')
two_core_data = readThroughputData('final/artificial_workload_2core')

plotThroughputs(single_core_data)
plotThroughputs(two_core_data)
"""
0
# -
# # Throughput plots II
#

"""
NANO_TO_SEC = 1000000000

def CleanLinesFromErrors(lines):
    
    i = 0
    while i < len(lines):
        first = lines[i]
        if not first.startswith("mpirun"):
            continue
        i = i+1
        err = False
        while i < len(lines) and lines[i].startswith("["):
            err = True
            i = i + 1
        if not err:
            second = lines[i]
            i = i + 1
            yield first, second
        
    

def GetThroughputData(experiment):
    with open(data_path + experiment, 'r') as f:
        lines = f.readlines()
        
    pairs = CleanLinesFromErrors(lines)
    
    #pairs = list(zip(lines[0::2], lines[1::2]))

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
"""
0


"""
experiments = ['/througput/n_hosts_througput.txt','/througput/one_host_througput.txt']

for experiment in experiments:
    PlotSingleOperionTimesPerRank(GetThroughputData(experiment), ["update/get", "put"],lable = experiment + ', Throughput for ', yLable= 'Operations per Second')
    

"""
0

# +
from __future__ import with_statement

def GetThroughputDataNew(experiment, dbs = [ 'papyrus', "paperless", 'Ipaperless'], max_rank = 24,  plot_data = pd.DataFrame(columns=['optime',  'batch_size', 'optype', 'op_ratio', 'vallen' ,'rank_size', 'run', 'mem_table_size','db']), mem_table_size=(None,0), ):
   

    for db in dbs:
        
        db_string = db
        if db == 'Ipaperless':
            sb_string = 'IGet paperless'
        
        for i in range(max_rank):
            if mem_table_size[0] == None:
                path = data_path + experiment + '/' + db + '/out' + str(i) + '.txt'
            else:
                path = data_path + experiment + '/' + db + '_' + str(mem_table_size[0])+ '/out' + str(i) + '.txt'
            
            try:
                lines = open(path, 'r').readlines()
            except FileNotFoundError:
                #print('oops file does not exists: ' + path)
                continue
            reg = re.compile("rank_count:([0-9]+),keylen:([0-9]+),vallen:([0-9]+),count:([0-9]+),update_ratio:([0-9]+),batch_size:([0-9]+),put_time:([0-9]+),get_update_time:([0-9]+)")

            
            run_nums = collections.defaultdict(int)
        
            for line in lines:
                reg_res = reg.match(line)
                if reg_res == None or reg_res.group(1) == None:
                    break

                rank_size = int(reg_res.group(1))
                vallen = int(reg_res.group(3))
                count = int(reg_res.group(4))
                ratio = int(reg_res.group(5))
                batch_size = int(reg_res.group(6))
                
                
                run = run_nums[rank_size, vallen, count,ratio,batch_size]
                run_nums[rank_size, vallen, count,ratio,batch_size] = run + 1
                
                put_through = int(reg_res.group(7))
                get_through = int(reg_res.group(8))

                put_through = count / put_through * NANO_TO_SEC * rank_size
                get_through = count / get_through* NANO_TO_SEC * rank_size

                plot_data = plot_data.append({'optime': put_through,
                                              'rank_size':rank_size,
                                              'vallen':vallen,
                                              'op_ratio': ratio,
                                              'batch_size': batch_size,
                                              'optype' : "put",
                                              'run' : run,
                                              'mem_table_size' : mem_table_size[1],
                                              'db': db}, ignore_index=True)
                plot_data = plot_data.append({'optime': get_through,
                                              'rank_size':rank_size,
                                              'vallen':vallen,
                                              'op_ratio': ratio,
                                              'batch_size': batch_size,
                                              'optype' : "update/get",
                                              'run' : run,
                                              'mem_table_size': mem_table_size[1],
                                              'db': db}, ignore_index=True)
                
    return plot_data.groupby(['rank_size','vallen', 'op_ratio', 'batch_size', 
                              'optype', 'run', 'mem_table_size', 'db']).sum().reset_index()
# -



# +
def PlotThroughput2(plot_data, optypes = ["update/get", "put"], vallens =  [131072], op_ratios = [0,5,50]):
    for vallen in vallens:
        for op_type in optypes:
            for op_ratio in op_ratios:
                #for ratio in ratios:
                plt.yscale('log')
                print('Throughput for ' + op_type+ ', opratio: ' + str(op_ratio) + ', with value length ' + str(vallen) + 'B')
                select = (plot_data['optype'] == op_type) & (plot_data['vallen'] == vallen) & (plot_data['op_ratio'] == op_ratio) 
                if op_type == "put":
                    select = select & (plot_data['db'] != 'paperless')
                sns.boxplot(data=plot_data[select], x='rank_size', y='optime', hue='db', showfliers = False)
                plt.yscale('log')
                plt.xlabel('Number of ranks')
                plt.ylabel('KPRS (kilo requests per second')
                plt.show()
            
def PlotThroughputStorage(plot_data, optypes = ["update/get", "put"], vallens =  [131072]):
    for vallen in vallens:
        for op_type in optypes:
            #for ratio in ratios:
            plt.yscale('log')
            print('Storage throughput for ' + op_type+ ', with value length ' + str(vallen) + 'B')
            select = (plot_data['optype'] == op_type) & (plot_data['vallen'] == vallen) 
            
            if(op_type == "put"):
                sns.boxplot(data=plot_data[select], x='mem_table_size', y='optime', hue='db', showfliers = False)
            else:            
                sns.boxplot(data=plot_data[select], x='mem_table_size', y='optime', hue='db', showfliers = False)
            
            plt.yscale('log')
            plt.xlabel('Memory Table size in percent of all data inserted')
            plt.ylabel('KPRS (kilo requests per second')
            plt.show()


# +
NANO_TO_SEC = 1000000000
storage_experiment = '/throughput_storage_report_n_host'
dbs = ['paperless', 'papyrus']
mem_table_sizes = [(6555200, 1), (173880000,25),(665520000,100) ]

data = pd.DataFrame(columns=['optime',  'batch_size', 'optype', 'op_ratio', 'vallen' ,'rank_size', 'run', 'mem_table_size', 'db'] )

for mem_tbl_size in mem_table_sizes:
    data = GetThroughputDataNew(storage_experiment, dbs=dbs, max_rank = 1, plot_data=data, mem_table_size=mem_tbl_size)

#with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#    print(data[data['optype'] == 'put'])
PlotThroughputStorage(data, vallens=[65536])
        


# +
NANO_TO_SEC = 1000000000
experiments = ['/throughput_report_n_host', '/throughput_report_one_host'] 

for experiment in experiments:
    data = GetThroughputDataNew(experiment, max_rank = 24)
    print(experiment)
    PlotThroughput2(data)
# -








# # Relaxed vs sequential mode

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

data_path = os.environ['PAPERLESS_KV_DATA_DIR'] + "/data/paperless-data"
NANO_TO_SEC = 1000000000
# % of ops are updates


# I put it into class to not collide with Julia's code.
class RelSeqBench:
    def __init__(self, experiment_name, rank_sizes, n_runs, count, mem_table_sizes, merge_mem_tables=False):
        self.experiment_name = experiment_name
        self.rank_sizes = rank_sizes
        self.n_runs = n_runs
        self.count = count
        self.mem_table_sizes = mem_table_sizes
        self.merge_mem_tables = merge_mem_tables

    def dataPathRel(self, rank_size, run_idx, mode, mem_table_size):
        return f"{data_path}/{self.experiment_name}/{mode}/ratio0/ranks{rank_size}/run{run_idx}/mt{mem_table_size}"
       
    def dataPathSeq(self, rank_size, run_idx, mode):
        return f"{data_path}/{self.experiment_name}/{mode}/ratio0/ranks{rank_size}/run{run_idx}"

    def throughputForRun(self, rank_size, run_idx, mode, mem_table_size = None):
        if mem_table_size:
            p = self.dataPathRel(rank_size, run_idx, mode, mem_table_size)
        else:
            p = self.dataPathSeq(rank_size, run_idx, mode)
        total_put_t = 0
        for i in range(rank_size):
            with open(f'{p}/put_total{i}.txt') as f:
                total_put_t = total_put_t + int(f.read())
        return (rank_size * self.count) / (total_put_t / NANO_TO_SEC)

    # The throughput value is (# of ops)/(sum of per-op timings).
    def readThroughputData(self):
        throughputs = pd.DataFrame(columns=['throughput', 'rank_size', 'mode'])
            
        for _, rank_size in enumerate(self.rank_sizes):
            for i in range(1, self.n_runs + 1):
                val = self.throughputForRun(rank_size=rank_size, run_idx=i, mode='seq')
                throughputs = throughputs.append({'throughput':val, 'rank_size':rank_size, 'mode':'SEQ'}, ignore_index=True)
        
        for (mem_table_size, percent) in self.mem_table_sizes:
            for _, rank_size in enumerate(self.rank_sizes):
                for i in range(1, self.n_runs + 1):
                    val = self.throughputForRun(rank_size=rank_size, run_idx=i, mode='rel',mem_table_size=mem_table_size)
                    if self.merge_mem_tables:
                        mode_string = 'REL'
                    else:
                        mode_string = 'REL: ' + str(percent) + '%' 
                    throughputs = throughputs.append({'throughput':val, 'rank_size':rank_size, 'mode': mode_string}, ignore_index=True)
        
            
        
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


bench = RelSeqBench('seq_vs_rel_2core_report_one_host/paperless', rank_sizes=[4, 8, 16, 24], n_runs=8, count=1000, mem_table_sizes = [(6555200, 1), (173880000,25),(665520000,100)], merge_mem_tables=True)
bench.plotThroughputs()

bench = RelSeqBench('seq_vs_rel_2core_report_one_host/papyrus', rank_sizes=[4, 8, 16, 24], n_runs=8, count=1000, mem_table_sizes = [(6555200, 1), (173880000,25),(665520000,100)], merge_mem_tables=True)
bench.plotThroughputs()



# # Hash vs Tree Cache
#

# +
experiment = 'cache_comp'
caches = ['hash', 'tree']
sizes = [8,32,512,1024,4096]
# 4096 is stupid since we do HardDisk access then
sizes = [1024]
ranks = [1]

for rank in ranks:
    plot_data = pd.DataFrame(columns=['optime', 'value_size', 'op_ratio' ,'rank_size', 'db'])
    plot_data = OpTimeForRunPerSize(experiment + '/hash', rank, 0, sizes, plot_data, 'hash', keysize_eq_value_size=True)
    plot_data = OpTimeForRunPerSize(experiment + '/tree', rank, 0, sizes, plot_data, 'tree', keysize_eq_value_size=True)
    PlotSingleOperionTimesDist(plot_data[plot_data['value_size'] == 1024], rank, ["localGet"], 'HashVSTreeCache')


# -



# # Vector Compare

# +
m_path = data_path + "/compare_vec/large_strides/result_cmp_memcmp.txt"
v_path = data_path + "/compare_vec/large_strides/result_cmp_vectorized.txt"

print(m_path)

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




# -

# # YCSB

# +
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import re

DATA_PATH = "data/paperless-data"

class YCSB:
    def __init__(self, experiment_name, dbs, rank_sizes, n_runs):
        self.experiment_name = experiment_name
        self.dbs = dbs
        self.rank_sizes = rank_sizes
        self.n_runs = n_runs

    def dataPath(self, db, rank_size, run_idx):
        return f"{DATA_PATH}/{self.experiment_name}/{db}/{rank_size}/{run_idx}"

    def runtimesForRun(self, db, rank_size, run_idx):
        rtimes = []
        with open(f'{self.dataPath(db, rank_size, run_idx)}/run.txt') as f:
            # [OVERALL], RunTime(ms), 
            rtimes_str = re.findall('\[OVERALL\], RunTime\(ms\), (\d+)', f.read())
            rtimes = list(map(lambda x : int(x)/1000, rtimes_str))
        return rtimes

    # The throughput value is (# of ops)/(sum of per-op timings).
    def readRuntimeData(self):
        rtimes = pd.DataFrame({'runtime': pd.Series([], dtype='int'),
                   'rank_size': pd.Series([], dtype='int'),
                   'db': pd.Series([], dtype='str')})
        for db in self.dbs:
            for rank_size in self.rank_sizes:
                for i in range(1, self.n_runs + 1):
                    for val in self.runtimesForRun(rank_size=rank_size, run_idx=i, db=db):
                        rtimes = rtimes.append({'runtime':val, 'rank_size':rank_size, 'db':db}, ignore_index=True)
        return rtimes


    def plotRuntimes(self):
        rtimes = self.readRuntimeData()
        print(rtimes.dtypes)
        # Only number the actual datapoints
        plt.title(f'MDHIM, PapyrusKV and Paperless weak scaling tests with YCSB')
        sns.lineplot(data=rtimes, x='rank_size', y='runtime', hue='db')
        # sns.swarmplot(data=throughputs[select], x='rank_size', y='throughput', hue='db')
        plt.xlabel('Number of ranks')
        plt.ylabel('Time in seconds (single rank)')
        plt.show()
# -

bench = YCSB('ycsb_Jan_12_01_58', rank_sizes=[4, 8, 16, 24], n_runs=3, dbs=["paperless", "papyrus"])
bench.plotRuntimes()
