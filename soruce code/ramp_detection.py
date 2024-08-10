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
import os
import numpy as np
from itertools import tee
from scipy import stats
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")
import math

directory = sys.argv[1]


def init(directory):
    edge_sum = pd.DataFrame(columns=('edge', 'source', 'target', 'entry_ts', 'duration', 'exit_ts'))
    ramp_edges = pd.DataFrame(columns=('edge', 't_test_stats', 't_test_pvalue'))
    paths = []
    for filepath in os.listdir(directory):
        path = directory + filepath
        paths.append(filepath)
    print(paths)

    for path in paths:
        paths_dir = directory + path
        if(path == "edge_sum1.txt"):
            edge_sum1_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum1 = edge_sum1_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum2.txt"):
            edge_sum2_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum2 = edge_sum2_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum3.txt"):
            edge_sum3_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum3 = edge_sum3_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum4.txt"):
            edge_sum4_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum4 = edge_sum4_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum5.txt"):
            edge_sum5_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum5 = edge_sum5_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
    ramp_candidates1 = ramp_time_window(edge_sum1, edge_sum2)
    ramp_candidates2 = ramp_time_window(edge_sum2, edge_sum3)
    ramp_candidates3 = ramp_time_window(edge_sum3, edge_sum4)
    ramp_candidates4 = ramp_time_window(edge_sum4, edge_sum5)

    frames = [ramp_candidates1, ramp_candidates2, ramp_candidates3,ramp_candidates4]
    #frames = [ramp_candidates1, ramp_candidates2]
    ramp_edges = pd.concat(frames)
    draw_duration_change_chart(ramp_edges)
    ramp_edges.to_csv('ramp_edges.txt', encoding='utf-8')
    ramp_edges_significance = check_significance(ramp_edges, 0.5)
    df_ramp_check = check_ramp(ramp_edges_significance)
    df_ramp= df_ramp_check.sort_values(['ramp'])
    ramp_edges_significance.to_csv('ramp_edges_significance.txt', encoding='utf-8')
    df_ramp.to_csv('ramp.txt', encoding='utf-8')


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

def draw_duration_change_chart(df):
    UniqueEdges = df.edge.unique()
    for i in range(len(UniqueEdges)):
        df_filtered = df[:][df.edge == UniqueEdges[i]]
        mean1 = df_filtered['edges1_mean']
        mean2= df_filtered['edges2_mean']
        sns.distplot(mean2, color='grey')
        sns.distplot(mean1, color='blue')
        plt.xlabel('Population duration')
        plt.ylabel('Probability Density')
        str_name= UniqueEdges[i]+'mean'
        plt.savefig(str_name, format="PNG")
        plt.clf()

        list=[]
        i_len= len(df_filtered['edges1_mean'])
        for i in range (1, i_len+1):
                list.append(i)

        ar = np.array(list)
        df_filtered['ar']= ar
        sns.distplot(df_filtered['edges1_mean'], df_filtered['ar'], color='blue')
        sns.distplot(df_filtered['edges2_mean'], df_filtered['ar'], color='grey')
        #plt.plot(df_filtered['edges1_mean'], df_filtered['ar'], 'x')
        str_name2= '2'+ str_name
        plt.savefig(str_name2, format="PNG")
        plt.clf()
        """
        # plot data
        fig, ax = plt.subplots(figsize=(15,7))
        # use unstack()
        df.groupby(['edge']).count()['edges1_mean'].plt.plot(ax=ax)
        str_name2= '2'+ str_name
        plt.savefig(str_name, format="PNG")
        plt.clf()
        """
def check_significance(df, t):
    significance_flag=[]
    for row in range(len(df)):
        if (math.isnan(df['t_test_pvalue'].iloc[row])):
            df['t_test_pvalue'].iloc[row]= 1
        if (int(df['t_test_pvalue'].iloc[row]) < t):
            significance_flag.append(1)
        else:
            significance_flag.append(0)
    df['significance'] = significance_flag
    return df

def ramp_time_window(df1, df2):
    df_ramp = pd.DataFrame(columns=('edge', 't_test_stats', 't_test_pvalue'))
    df1.columns = df2.columns.str.strip()
    df2.columns = df2.columns.str.strip()

    bootstrapped_duration1 = []
    bootstrapped_duration2 = []
    t_test_stats = []
    t_test_pvalue = []
    edges1_dur = []
    edges2_dur = []
    edges1_mean=[]
    edges2_mean = []

    UniqueEdges1 = df1.edge.unique()
    UniqueEdges2 = df2.edge.unique()
    intersect_edges= np.intersect1d(UniqueEdges1, UniqueEdges2)
    for i in range(len(intersect_edges)):
        df2_filtered = df2[:][df2.edge == intersect_edges[i]]
        print("df2_filtered")
        print(df2_filtered)
        edges2_dur = df2_filtered['duration']
        edges2_dur = np.array(edges2_dur)
        df1_filtered = df1[:][df1.edge == intersect_edges[i]]
        print("df1_filtered")
        print(df1_filtered)
        edges1_dur = df1_filtered['duration']
        edges1_dur = np.array(edges1_dur)
        print(edges1_dur)
        print("Population Mean")
        print("---------------")
        edges_mean2= edges2_dur.mean()
        edges_mean1= edges1_dur.mean()
        edges1_mean.append(edges_mean1)
        edges2_mean.append(edges_mean2)
        if(edges_mean1 and edges_mean2):
            sns.distplot(edges2_dur, color='grey')
            sns.distplot(edges1_dur, color='blue')
            plt.xlabel('Population duration')
            plt.ylabel('Probability Density')
            plt.savefig("mean", format="PNG")
            plt.clf()
        mean1, mean2 = calc_sample_mean(50, 5000, edges1_dur, edges2_dur)
        sns.distplot(mean1, color='r')
        sns.distplot(mean2, color='g')
        plt.xlabel('Sample duration (Sample size =2)')
        plt.ylabel('Probability Density')
        plt.savefig("sample", format="PNG")
        plt.clf()

        statistic, pvalue = t_test(mean1, mean2)
        # statistic, pvalue = t_test(r1, r2)
        t_test_stats.append(statistic)
        t_test_pvalue.append(pvalue)
    df_ramp['edge']= intersect_edges
    df_ramp['t_test_stats'] = t_test_stats
    df_ramp['t_test_pvalue'] = t_test_pvalue
    df_ramp['edges1_mean']= edges1_mean
    df_ramp['edges2_mean'] = edges2_mean
    return df_ramp

def calc_sample_mean(sample_size, no_of_sample_means, r1, r2):
    mean1=[]
    mean2 = []
    for i in range(no_of_sample_means):
        sample2_duration = np.random.choice(r2, sample_size)
        sample2_mean = sample2_duration.mean()
        mean2.append(sample2_mean)
        sample1_duration = np.random.choice(r1, sample_size)
        sample1_mean=sample1_duration.mean()
        mean1.append(sample1_mean)
    return mean1, mean2
"""
If the T-test’s corresponding p-value is .03, then a statistically significant relationship would be implied. 
There is only a 3% probability the null hypotesis is correct 
(and the results are random). Therefore, we reject the null hypothesis, and accept the alternative hypothesis.
"""


def t_test(br1, br2):
    statistic, pvalue = stats.ttest_ind(br1, br2)
    return statistic, pvalue

def check_ramp(df):
    df_ramp = pd.DataFrame(columns=('edge', 'ramp'))
    ramp= []
    UniqueEdges = df.edge.unique()
    for i in range(len(UniqueEdges)):
        df_filtered = df[:][df.edge == UniqueEdges[i]]
        print("df_filtered")
        print(df_filtered)
        if(df_filtered['significance'].all()== 0):
            ramp.append(0)
        else:
            ramp.append(1)
    df_ramp['edge']= UniqueEdges
    df_ramp['ramp']= ramp
    return df_ramp

init(directory)

