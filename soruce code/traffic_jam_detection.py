"""
detection strategy: t-test
exactly the same as ramp detection method, but with different data
for traffic jam we run 5 experiments, for each experiment we increase the load and measure response times (no-need to record hugh data, just increase load and trace for a short time))
if for all experiments t-test shows a significant increase in response time in comparision with the prevoius experiment it shows that we have traffic_jam problem
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

    """
    DataFrameDict = {elem: pd.DataFrame() for elem in paths}

    for path in paths:
        paths_dir = directory + path
        edge_sum_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
        edge_sum = edge_sum_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        print(edge_sum.columns)
        print(edge_sum)
        DataFrameDict[path] = edge_sum

    for key1, key2 in pairwise(DataFrameDict.keys()):
        df1 = DataFrameDict[key1]
        df2 = DataFrameDict[key2]
        df1.head(5)
        ramp_candidates = ramp_time_window(df1, df2)
        ramps = [ramp_edges, ramp_candidates]
        ramp_edges = pd.concat(ramps)
        ramp_edges_significance= check_significance(ramp_edges, 0.05)
    df_traffic_jam_check= check_traffic_jam(ramp_edges_significance)
    df_traffic_jam= df_traffic_jam_check.sort_values(['traffic_jam'])
    ramp_edges_significance.to_csv('traffic_jam_edges_significance.txt', encoding='utf-8')
    df_traffic_jam.to_csv('traffic_jam.txt', encoding='utf-8')
    """

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
        elif(path == "edge_sum6.txt"):
            edge_sum6_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum6 = edge_sum6_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum7.txt"):
            edge_sum7_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum7 = edge_sum7_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum8.txt"):
            edge_sum8_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum8 = edge_sum8_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum9.txt"):
            edge_sum9_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum9 = edge_sum9_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum10.txt"):
            edge_sum10_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum10 = edge_sum10_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum11.txt"):
            edge_sum11_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum11 = edge_sum11_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])
        elif(path == "edge_sum12.txt"):
            edge_sum12_df = pd.read_csv(paths_dir, sep=',', lineterminator='\n')
            edge_sum12 = edge_sum12_df.drop(columns=['Unnamed: 0', 'source', 'target', 'entry_ts', 'exit_ts'])

    ramp_candidates1 = traffic_jam_time_window(edge_sum1, edge_sum2)
    ramp_candidates2 = traffic_jam_time_window(edge_sum2, edge_sum3)
    ramp_candidates3 = traffic_jam_time_window(edge_sum3, edge_sum4)
    ramp_candidates4 = traffic_jam_time_window(edge_sum4, edge_sum5)
    ramp_candidates5 = traffic_jam_time_window(edge_sum5, edge_sum6)
    ramp_candidates6 = traffic_jam_time_window(edge_sum6, edge_sum7)
    ramp_candidates7 = traffic_jam_time_window(edge_sum7, edge_sum8)
    ramp_candidates8 = traffic_jam_time_window(edge_sum8, edge_sum9)
    ramp_candidates9 = traffic_jam_time_window(edge_sum9, edge_sum10)
    ramp_candidates10 = traffic_jam_time_window(edge_sum10, edge_sum11)
    ramp_candidates11 = traffic_jam_time_window(edge_sum11, edge_sum12)

    frames = [ramp_candidates1, ramp_candidates2, ramp_candidates3,ramp_candidates4,  ramp_candidates5,
              ramp_candidates6, ramp_candidates7, ramp_candidates8, ramp_candidates9, ramp_candidates10, ramp_candidates11]
    #frames = [ramp_candidates1, ramp_candidates2]
    ramp_edges = pd.concat(frames)
    draw_duration_change_chart(ramp_edges)
    ramp_edges.to_csv('ramp_edges.txt', encoding='utf-8')
    ramp_edges_significance = check_significance(ramp_edges, 0.9)
    df_traffic_jam_check = check_traffic_jam(ramp_edges_significance)
    df_traffic_jam= df_traffic_jam_check.sort_values(['traffic_jam'])
    ramp_edges_significance.to_csv('traffic_jam_edges_significance.txt', encoding='utf-8')
    df_traffic_jam.to_csv('traffic_jam.txt', encoding='utf-8')


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

def traffic_jam_time_window(df1, df2):
    df_traffic_jam = pd.DataFrame(columns=('edge', 't_test_stats', 't_test_pvalue'))
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
    df_traffic_jam['edge']= intersect_edges
    df_traffic_jam['t_test_stats'] = t_test_stats
    df_traffic_jam['t_test_pvalue'] = t_test_pvalue
    df_traffic_jam['edges1_mean']= edges1_mean
    df_traffic_jam['edges2_mean'] = edges2_mean
    return df_traffic_jam

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

def check_traffic_jam(df):
    df_traffic_jam = pd.DataFrame(columns=('edge', 'traffic_jam'))
    traffic_jam= []
    UniqueEdges = df.edge.unique()
    for i in range(len(UniqueEdges)):
        df_filtered = df[:][df.edge == UniqueEdges[i]]
        print("df_filtered")
        print(df_filtered)
        if(df_filtered['significance'].all()== 1):
            traffic_jam.append(1)
        else:
            traffic_jam.append(0)
    df_traffic_jam['edge']= UniqueEdges
    df_traffic_jam['traffic_jam']= traffic_jam
    return df_traffic_jam

init(directory)

