import os
import pandas as pd
import numpy as np
import math
from os import walk

mpath= __file__
df_aggregation = pd.DataFrame(columns=('analysis_time', 'path', 'count', 'mean_duration'), dtype=int)

def build_agg(file1, file2, df_aggr):
    #traces_df1 = pd.DataFrame(columns=('caller','callee', 'count', 'mean_duration'),  dtype=int)
    #traces_df2 = pd.DataFrame(columns=('caller','callee', 'count', 'mean_duration'),  dtype=int)
    traces_df1 = pd.DataFrame(columns=('path', 'count', 'duration', 'str_path'), dtype=int)
    traces_df2 = pd.DataFrame(columns=('path', 'count', 'duration', 'str_path'), dtype=int)

    #traces_df_agg=pd.DataFrame(dtype=int)
    #traces_df1 = pd.read_csv(file1)
    #traces_df1['path']= traces_df1["caller"] + traces_df1["callee"]

    traces_df1= file1
    traces_df2 = file2
    #traces_df1.sort_values(by=['path'])
    #traces_df2['path']= traces_df2["caller"] + traces_df2["callee"]
    #traces_df2.sort_values(by=['path'])
    traces_df1.rename(columns={'count': 'count_x', 'duration': 'duration_x'})
    traces_df2.rename(columns={'count': 'count_y', 'duration': 'duration_y'})
    traces_df_agg = pd.merge(traces_df1, traces_df2, on= 'str_path',how='inner')
    print(traces_df_agg)


    for row in range(len(traces_df_agg)):
        count1 = traces_df_agg['count_x'].iloc[row]
        count2 = traces_df_agg['count_y'].iloc[row]
        mean_dur1 = traces_df_agg['duration_x'].iloc[row]
        mean_dur2 = traces_df_agg['duration_y'].iloc[row]
        str_path = traces_df_agg['str_path'].iloc[row]
        if (len(str(count1))) != 0 and (len(str(count2))) != 0:
            dict1 = {'analysis_time': file1, 'str_path': str_path, 'count': count1, 'duration': mean_dur1}
            dict2 = {'analysis_time': file2, 'str_path': str_path, 'count': count2, 'duration': mean_dur2}
            df_aggr= df_aggr.append(dict1, ignore_index=True)
            df_aggr= df_aggr.append(dict2, ignore_index=True)
        elif (len(str(count1))) != 0 and (len(str(count2))) == 0:
            dict1 = {'analysis_time': file1, 'str_path': str_path, 'count': count1, 'duration': mean_dur1}
            df_aggr= df_aggr.append(dict1, ignore_index=True)
        elif (len(str(count1))) == 0 and (len(str(count2))) != 0:
            dict1 = {'analysis_time': file2, 'str_path': str_path, 'count': count2, 'duration': mean_dur2}
            df_aggr= df_aggr.append(dict1, ignore_index=True)
    return df_aggr

from collections import Counter
def stats_cal(df_agg):
    print('-'*30)
    counter = Counter(e for e in df_agg['str_path'])
    d = dict(counter)
    stats_count = df_agg.groupby(['str_path'])['count'].agg(['mean', 'count', 'std'])
    print(stats_count)
    print(stats_count.columns)
    print('-'*30)
    ci95_hi_count = []
    ci95_lo_count = []

    for i in stats_count.index:
        m, c, s = stats_count.loc[i]
        ci95_hi_count.append(m + 1.96*s/math.sqrt(c))
        ci95_lo_count.append(m - 1.96*s/math.sqrt(c))

    stats_count['ci95_hi'] = ci95_hi_count
    stats_count['ci95_lo'] = ci95_lo_count
    print(stats_count)
    print('-'*30)

    stats_dur = df_agg.groupby(['str_path'])['duration'].agg(['mean', 'count', 'std'])
    print(stats_dur)
    print('-'*30)
    ci95_hi_dur = []
    ci95_lo_dur = []

    for i in stats_dur.index:
        m, c, s = stats_dur.loc[i]
        ci95_hi_dur.append(m + 1.96*s/math.sqrt(c))
        ci95_lo_dur.append(m - 1.96*s/math.sqrt(c))

    stats_dur['ci95_hi'] = ci95_hi_dur
    stats_dur['ci95_lo'] = ci95_lo_dur

    stats_count.to_csv('stats_count.txt', encoding='utf-8')
    stats_dur.to_csv('stats_dur.txt', encoding='utf-8')

def check_cf_in_data(df_agg):
    stats_count = pd.read_csv("stats_count.txt")
    stats_dur = pd.read_csv("stats_dur.txt")
    cnt_not_in_than_ci95=[]
    for e in range(len(stats_count)):
        str_path= stats_count['str_path'].iloc[e]
        for row in range(len(df_agg)):
            if df_agg['str_path'].iloc[row] == str_path:
                if (df_agg['count'].iloc[row] > stats_count['ci95_hi'].iloc[e]) | (df_agg['count'].iloc[row] < stats_count['ci95_lo'].iloc[e]):
                    cnt_not_in_than_ci95.append(1)
                else:
                    cnt_not_in_than_ci95.append(0)
    df_agg['cnt_not_in_than_ci95']= cnt_not_in_than_ci95

    dur_not_in_than_ci95= []
    for e in range(len(stats_dur)):
        str_path = stats_dur['str_path'].iloc[e]
        for row in range(len(df_agg)):
            if df_agg['str_path'].iloc[row] == str_path:
                if df_agg['duration'].iloc[row] > stats_dur['ci95_hi'].iloc[e] or df_agg['duration'].iloc[row] < \
                        stats_dur['ci95_lo'].iloc[e]:
                    dur_not_in_than_ci95.append(1)
                else:
                    dur_not_in_than_ci95.append(0)
    df_agg['dur_not_in_than_ci95']= dur_not_in_than_ci95

    freq_in_whole_trace=[]
    total_count = df_agg['count'].sum()
    total= total_count * 0.6
    df_total = df_agg.groupby(["str_path"]) \
        .agg({'count': 'sum'}) \
        .reset_index()

    print(df_total)
    for row in range(len(df_total)):
        str_path= df_total['str_path'].iloc[row]
        for i in range (len(df_agg)):
            if df_agg['str_path'].iloc[i] == str_path:
                if df_total['count'].iloc[row] > total:
                    freq_in_whole_trace.append(1)
                    print(str_path)
                else:
                    freq_in_whole_trace.append(0)
    df_agg['freq_in_whole_trace'] = freq_in_whole_trace
    df_agg.to_csv('df_agg.txt', encoding='utf-8')
    # check if in the whole trace the edge freq or dur is abnormal (abnormal portion of frequency and duration) then if ci95 freq and dur is okay for the same, put flag for disable tracing
    # if freq > 0.9 whole freq
    """
    for e in range(len(stats_dur)):
        high = stats_dur['ci95_hi'].iloc[e]
        low = stats_dur['ci95_lo'].iloc[e]
        edge2 = stats_dur['edge'].iloc[e]
        for row in range(len(df_agg)):
            if df_agg['edge'].iloc[row] == edge2:
                df_agg['dur_not_in_than_ci95'] = df_agg['mean_duration'].apply(lambda x: 'True' if x < low or x > high else 'False')

    """
"""
df_aggregation1= build_agg('113122.csv', '070611.csv', df_aggregation)
df_aggregation2= build_agg('215800.csv', '220456.csv', df_aggregation)
df_aggregation3= build_agg('221142.csv', '221821.csv', df_aggregation)
df_aggregation4= build_agg('222415.csv', '223047.csv', df_aggregation)
df_aggregation5= build_agg('223412.csv', '080712.csv', df_aggregation)
df_aggregation6= build_agg('081450.csv', '082253.csv', df_aggregation)

frames = [df_aggregation1, df_aggregation2, df_aggregation3, df_aggregation4, df_aggregation5, df_aggregation6]
result = pd.concat(frames)
result.to_csv('result.txt', encoding='utf-8')
print('-' * 30)
print(result.shape)
stats_cal(result)
check_cf_in_data(result)
"""
"""
def pairwise(iterable):
    "s -> (s0, s1), (s2, s3), (s4, s5), ..."
    a = iter(iterable)
    return zip(a, a)

def exe_trace(mypath):
    filenames = next(walk(mypath), (None, None, []))[2]  # [] if no file
    substring= ".csv"
    csv_filenames = [string for string in filenames if substring in string]
    frames=[]
    i=0
    for x, y in pairwise(csv_filenames):
        frames[i]= build_agg(x, y)
        i=+1
    result = pd.concat(frames)
    print('-' * 30)
    print(result.shape)
    stats_cal(result)
    check_cf_in_data(result)

exe_trace(mpath)
"""