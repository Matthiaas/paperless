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

# + id="zeWCU8XvnRBH"
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

import os


# + id="OFvbZi__9c_j"
def readVector(file_name):
  return np.loadtxt(file_name)

# + [markdown] id="cALaPDztnH_X"
# # Paperless and Papyrus plots
#

# + id="BJa3a46qnhdl"
path = os.environ['PAPERLESS_KV_DATA_DIR'] + "/data/512MB/"
print(path)
ranks = [1,2,8]

# + colab={"base_uri": "https://localhost:8080/", "height": 539} id="9vWxik0KpqO8" outputId="4b83a218-c7db-484c-ac74-14a304f019a0"
gets = []
puts = []
plt.figure(num=None, figsize=(15, 6), dpi=80, facecolor='w', edgecolor='k')

for rank in ranks:
  for i in range(8):
    print("--------------------------------------")
  print("Plots for ranks n=" + str(rank))
  for i in range(rank):
        
    plt.figure(num=None, figsize=(15, 6* rank), dpi=80, facecolor='w', edgecolor='k')
    
    gets.append(readVector(path + str(rank) + "/get" + str(i) + ".txt"))
    puts.append(readVector(path + str(rank) + "/put" + str(i) + ".txt"))

    plt.subplot(rank, 2, 2*i +1)
    plt.yscale('log')
    plt.scatter(list(range(0, len(puts[i]))), puts[i], 0.5)
    plt.title("Put rank" + str(i))
    plt.ylabel("time")
    plt.xlabel("nth operation")
    # plt.show()
    
    plt.subplot(rank, 2, 2*i + 2)

    # #plt.plot(gets[i])
    plt.yscale('log')
    plt.scatter(list(range(0, len(gets[i]))), gets[i], 0.5)
    plt.ylabel("time")
    plt.xlabel("nth operation")
    plt.title("Get rank" + str(i))
    # plt.show()
  plt.show()


# + colab={"base_uri": "https://localhost:8080/", "height": 1000} id="pdmV9lquni-5" outputId="03cbd4a8-cf53-4e59-cdcc-b4d39a791bdf"



# + id="5t-v0JFD_Js9"

