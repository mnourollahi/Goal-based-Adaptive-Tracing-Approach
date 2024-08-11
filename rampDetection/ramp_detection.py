"""
goal: ramp detection --> an increase in response time over operation time

input:
1. a dataset D consisting of each thread's execution paths  (path duration times series)/ (func duration times series) --> so for each path dataset D= path time series (resp_time, time_stamp)
2. slope threshold ST
3. s, which is significance level for statistical tests (which is set to 5 in accordance to 95% confidence interval)

output: set of paths/funcs that are ramp candidates, ramp(path/func, start_time_stamp, end_time_stamp, load)

detection strategy:
measurement data:
same as perf-Problem
run a heavy Load test

Linear regression:
identify whether dataset shows an increasing trend --> evaluation of the corresponding slope
1. for each path/func apply linear regression on the time series of response times (dataset D) to extract slope K of the corresponding linear curve.
2. rank ramp anti_pattern candidates based on slope k value of the path/func. if the slope K is greater that ST threshold, the path/func is considered as ramp anti_pattern

Direct growth strategy:
there is an occurance of ramp if there is a significant difference between response time of the begining and end of the loadtest
1. choronologically divide the response time series into two subsets R1 and R2 for which the mean values r1 and r2 are calculated.
2. compare samples R1 and R2 applying a t-test
3. to perform step 2 first to make sure that R1 and R2 are normally distributed apply "central limit theorem" which gives two bootstrapped sets B1 and B2.
4. apply t-test on B1 and B2, to get a p-value p.
5. if p is smaller than s and the same time r1<r2, then response times in R2 are smaller than R1. in this case the path/func is a candidate for ramp

Time Window Strategy:
test data gathering method:
run load test to warm-up system, then gather traces in base-loads
increase load in 3 steps:
1. gather base load trace
2. put load
3. gather base load trace
4. put heavier load
5. gather base load trace
6. put heavier load
7. gather base load trace

there is an occurance of ramp if there is a significant difference between response time of the begining and end of the loadtest
1. for each path/func a pairwise comparison of the response time series of neighbouring experiments is conducted (step 1 and 3, 3 and 5, 5 and 7)
2. for this comparision response time sets for each experiment is gathered and bootstrapped to be normally distributed. then a t-test is applied to compare same as direct growth
3. if comparision of all sets results in significant growth of response time, then ramp anti-pattern is observed

"""
import sys
import pandas as pd
import os
import math
from matplotlib import pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from itertools import tee
from scipy import stats
import random

directory = sys.argv[1]

def init(directory):
    edge_sum = pd.DataFrame(columns=('edge', 'source', 'target', 'entry_ts', 'duration', 'exit_ts'))
    ramp_edges= pd.DataFrame(columns=('edge', 't_test_duration'))
    paths= []
    for filepath in os.listdir(directory):
        path = directory + filepath
        paths.append(filepath)
    print(paths)

    DataFrameDict = {elem: pd.DataFrame() for elem in paths}

    for path in paths:
        paths_dir= directory + path
        edge_sum_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
        edge_sum = edge_sum_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        print(edge_sum.columns)
        print(edge_sum)
        DataFrameDict[path] = edge_sum
        #DataFrameDict[path]['duration'] = pd.to_numeric(DataFrameDict[filepath]['duration'])
    """

    DataFrameDict = {elem: pd.DataFrame() for elem in paths}

    for i in range(0, len(DataFrameDict)):
        path = directory + filepath
        edge_sum_df = pd.read_csv(path, sep=',', lineterminator='\n')
        edge_sum= edge_sum_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        print(edge_sum.columns)
        print(edge_sum)
        DataFrameDict[filepath] = edge_sum
        DataFrameDict[filepath]['duration'] = pd.to_numeric(DataFrameDict[filepath]['duration'])

        print(path)
        print(filepath)
        print(DataFrameDict[filepath])

    print(DataFrameDict)
    print()
    """

    for key1, key2 in pairwise(DataFrameDict.keys()):
        df1= DataFrameDict[key1]
        df2 = DataFrameDict[key2]
        df1.head(5)
        ramp_candidates = ramp_time_window(df1, df2)
        ramps = [ramp_edges, ramp_candidates]
        ramp_edges = pd.concat(ramps)
    #ramp_edges.sort_values(['t_test'])
    ramp_edges.to_csv('ramp_edges.txt', encoding='utf-8')

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

def ramp_time_window(df1,df2):
    df1.columns = df1.columns.str.strip()
    df2.columns = df2.columns.str.strip()

    print(df1)

    df1_edge_meandur = df1.groupby(['edge'],  as_index=False).agg(mean_dur=pd.NamedAgg(column="duration", aggfunc="mean"))
    df2_edge_meandur = df2.groupby(['edge'],  as_index=False).agg(
        mean_dur=pd.NamedAgg(column="duration", aggfunc="mean"))

    length = max(len(df1_edge_meandur), len(df2_edge_meandur))
    #df_t_test = compare_direct_growth(df1_edge_meandur, df2_edge_meandur)

    bootstrapped_duration1= []
    bootstrapped_duration2 = []
    t_test_stats = []
    t_test_pvalue = []
    edges1_dur=[]
    edges2_dur=[]

    for row in range(0, length):
        if (row < len(df1_edge_meandur)) and (row < len(df2_edge_meandur)):
            r1 = df1_edge_meandur['mean_dur'].iloc[row]
            r2 = df2_edge_meandur['mean_dur'].iloc[row]
        br1, br2 = bootstrap_central_limit(r1, r2)
        bootstrapped_duration1.append(br1)
        bootstrapped_duration2.append(br2)
        statistic, pvalue = t_test(br1, br2)
        # statistic, pvalue = t_test(r1, r2)
        t_test_stats.append(statistic)
        t_test_pvalue.append(pvalue)
    """
    UniqueEdges = df2.edge.unique()
    for i in range(len(UniqueEdges)):
        df2_filtered = df2[:][df2.edge == UniqueEdges[i]]
        edges2_dur = df2_filtered['duration']
        len2= len(df2_filtered)
        if(UniqueEdges[i] in df1['edge'].unique()):
            df1_filtered = df1[:][df1.edge == UniqueEdges[i]]
            print("df1_filtered")
            print(df1_filtered)
            edges1_dur= df1_filtered['duration']
        else:
            edges1_dur = [0 for i in range(0, len2)]
            edges1_dur=  np.array(edges1_dur)

        br1, br2 = bootstrap_central_limit(edges1_dur, edges2_dur)
        bootstrapped_duration1.append(br1)
        bootstrapped_duration2.append(br2)
        statistic, pvalue = t_test(br1, br2)
        # statistic, pvalue = t_test(r1, r2)
        t_test_stats.append(statistic)
        t_test_pvalue.append(pvalue)
    df1.to_csv('df1.txt', encoding='utf-8')
    df2.to_csv('df2.txt', encoding='utf-8')
    """
    if len(df1) < len(df2):
        # df1['bootstrapped_duration'] = bootstrapped_duration1
        df1['t_test_stats'] = pd.Series(t_test_stats)
        df1['t_test_pvalue'] = pd.Series(t_test_pvalue)
        return df1
    else:
        # df2['bootstrapped_duration']= bootstrapped_duration2
        df2['t_test_stats'] = t_test_stats
        df1['t_test_pvalue'] = t_test_pvalue
        return df2

    #return df_t_test

def compare_direct_growth(df1,df2):
    length = max(len(df1), len(df2))
    bootstrapped_duration1= []
    bootstrapped_duration2 = []
    t_test_stats = []
    t_test_pvalue = []

    for row in range(0, length):
        if (row < len(df1)) and (row < len(df2)):
            r1= df1['mean_dur'].iloc[row]
            r2= df2['mean_dur'].iloc[row]

            br1, br2 = bootstrap_central_limit(list(r1), list(r2))
            bootstrapped_duration1.append(br1)
            bootstrapped_duration2.append(br2)
            #df1.at[row, 'bootstrapped_duration'] = br1
            #df2.at[row, 'bootstrapped_duration'] = br2
            print("this is br1\n")
            print(br1)
            statistic, pvalue = t_test(br1, br2)
            #statistic, pvalue = t_test(r1, r2)
            t_test_stats.append(statistic)
            t_test_pvalue.append(pvalue)

    df1.to_csv('df1.txt', encoding='utf-8')
    df2.to_csv('df2.txt', encoding='utf-8')

    if len(df1) < len(df2):
        #df1['bootstrapped_duration'] = bootstrapped_duration1
        df1['t_test_stats'] = t_test_stats
        df1['t_test_pvalue'] = t_test_pvalue
        return df1
    else:
        #df2['bootstrapped_duration']= bootstrapped_duration2
        df2['t_test_stats'] = t_test_stats
        df1['t_test_pvalue'] = t_test_pvalue
        return df2


"""
    merged_df = pd.concat([df1, df2], axis=1)

    UniqueEdges1 = df1.edge.unique()
    UniqueEdges2 = df2.edge.unique()

    length= max(len(UniqueEdges1), len(UniqueEdges2))

    for i in range(0, length):
        if i< len(UniqueEdges1):
            df_edge1 = df1[:][df1.edge == UniqueEdges1[i]]
            r1 = df_edge1['mean_dur']

        if i < len(UniqueEdges2):
            df_edge2 = df2[:][df2.edge == UniqueEdges2[i]]
            r2 = df_edge2['mean_dur']

        br1, br2= bootstrap_central_limit(r1,r2)
        df1['bootstrapped_duration']= br1
        df2['bootstrapped_duration'] = br2
        tr1, tr2 = t_test(br1, br2)
        df1['t_test_duration'] = tr1
        df2['t_test_duration'] = tr2
 

If the T-test’s corresponding p-value is .03, then a statistically significant relationship would be implied. 
There is only a 3% probability the null hypotesis is correct 
(and the results are random). Therefore, we reject the null hypothesis, and accept the alternative hypothesis.
"""
def t_test(br1,br2):
    statistic, pvalue= stats.ttest_ind(br1, br2)
    return statistic, pvalue

def bootstrap_central_limit(r1,r2):
    br1 = []
    br2 = []

    
    for _ in range(5000):
        br1.append(r1.mean())
        br2.append(r2.mean())
    """
    for i in range(5000):
        x = random.sample(r1.tolist(), 1)
        avg = np.mean(x)
        br1.append(avg)

        y = random.sample(r2.tolist(), 1)
        avg2 = np.mean(y)
        br2.append(avg2)

    print(np.mean(br1))
    print(np.mean(br2))
    """
    return br1,br2


init(directory)


