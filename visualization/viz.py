#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 10:49:45 2019

@author: dpanugroho
"""

import pandas as pd
import os 
import seaborn as sns
from matplotlib import pyplot as plt

def parse_json_result(filename):
    result_json = pd.read_json(filename)[["params","primaryMetric"]]
    params = [int(res["lineItemFilePath"] \
              .split(".arrow")[0] \
              .split("lineitem")[1] \
              .replace("k", "000")) for res in result_json["params"]]
    score = [res["score"]/1000000 for res in result_json["primaryMetric"]]
    sut = [filename.split("jmh-result-")[1].split(".json")[0].lower()] * len(score)
    version = [filename.split("jmh-result-")[1].split(".json")[0].lower().split("-")[0]] * len(score)
    result_df = pd.DataFrame({"version":version,"sut":sut, "sf":params, "ops/sec":score})
    return result_df

def get_result(result_directory, version="", max_sf=30000):
    final_result = pd.DataFrame()
    for file in os.listdir(result_directory):
        if ((file.endswith(".json")) and (version in file)):
            if len(final_result) == 0:
                final_result = parse_json_result(file)
            else:
                final_result = final_result.append(parse_json_result(file))
    
    return final_result[final_result["sf"]<max_sf]

def plot_result(data,outfname):
    plt.figure(figsize=(15,8))
    sns.set()
    sns.set_palette("husl",3)
    ax = sns_plot = sns.lineplot(x="sf", 
                            y="ops/sec", 
                            hue="sut", 
                            style="version", 
                            markers=True, data=data)
    ax.set(xscale="log", xlabel='scale factor', ylabel='avg execution time (ms)')
    plt.savefig(outfname, format='svg', dpi=1200)
    plt.show()
    
if __name__ == '__main__':
    all_results = get_result(".")
    old_result = get_result(".","incmemload")
    new_result = get_result(".","excmemload")
    
    plot_result(all_results, "graalvm-benchmark-on-tpchq6-all.svg")
    plot_result(old_result, "graalvm-benchmark-on-tpchq6-incmemload.svg")
    plot_result(new_result, "graalvm-benchmark-on-tpchq6-excmemload.svg")

