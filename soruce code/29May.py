import bt2
import sys
import pandas as pd
import networkx as nx
import subprocess
import matplotlib.pyplot as plt
from collections import deque
import time
# import graph as graphgen
import aggregation as agg
from itertools import tee
import numpy as np

directory = sys.argv[1]

df_func_sum = pd.DataFrame(columns=('start_time', 'file_func', 'func_name', 'mean_dur', 'max_dur', 'count'))
df_trace_sum = pd.DataFrame(columns=('start_time', 'end_time', 'workflow', 'wf_dur', 'wf_dur', 'wf_count'))
df_rank_funcs= pd.DataFrame(columns=('func_rank', 'func_name', 'func_score'))
df_rank_wf= pd.DataFrame(columns=('wf_rank', 'wf_name', 'wf_score'))

def init():
    for filepath in os.listdir(directory):
        df= iterate_traces_to_df(filepath)
        #divid_tid(df)
        sum_func(df)
        #call_stack_df= build_call_stack(df)
        #graph_df= build_graph(call_stack_df)

def func_rank_cal(df_func_sum):

    return df_rank_funcs

def wf_rank_cal(df_wf_sum):

    return df_rank_wf

def sum_func(df_func):
    df_func_stats= cal_func_stats(df_func)
    #write to a file without overwriting
    with open("sum_func.txt", "a") as text_file:
        text_file.write(df_func_stats + "\n")
        df_func_sum = text_file.read()

    return df_func_sum


def sum_wf(df_trace):
    #write to a file without overwriting
    with open("sum_wf.txt", "a") as text_file:
        text_file.write(df_trace + "\n")
    return df_trace_sum

def iterate_traces_to_df(filepath):
    # Create a trace collection message iterator with this path.
    df = pd.DataFrame(columns=('e_name', 'e_type', 'cur_ts', 'file_func', 'func_func', 'loc_func', 'tid', 'pid'))
    msg_it = bt2.TraceCollectionMessageIterator(filepath)
    # Iterate the trace messages.
    for msg in msg_it:
        if type(msg) is bt2._EventMessageConst:
            event = msg.event
            # Event  timestamp (ns)
            cur_ts = msg.default_clock_snapshot.ns_from_origin
            type(cur_ts)
            # threadid_fuc= event.packet.context_field['pthread_id']
            df.loc[msg] = [event.name, event['event_type_field'], cur_ts, event['file_name_field'],
                           event['func_name_field'],
                           event['loc_field'], event['vtid'], event['vpid']]
    return df

def divid_tid(trace_df):
    df_enhanced = pd.DataFrame(columns=('e_name', 'e_type', 'entry_ts', 'exit_ts', 'file_func', 'func_func', 'loc_func',
                                        'tid', 'pid', 'caller_row', 'caller_file', 'caller_func', 'duration_func'))
    frames = []
    tids = trace_df.tid.unique()
    print("tids")
    print(tids)
    tid_dict = {elem: pd.DataFrame() for elem in tids}
    en_tid_dict = {elem: pd.DataFrame() for elem in tids}

    for key in (tid_dict.keys()):
        tid_dict[key] = trace_df[:][trace_df.tid == key]
        df = tid_dict[key]
        df_en = build_call_stack(df, key)
        en_tid_dict[key] = df_en
        dfs = [df_enhanced, df_en]
        df_enhanced = pd.concat(dfs)
        df_sorted = df_en.sort_values('entry_ts')
        root = df_sorted.iloc[0]['func_func']
        graph = build_graph(df_en, key)
        # find_paths(graph)
        df_summerize = find_count_duration_path(graph, key, root)
    cal_func_stats(df_enhanced)
    df_summerize.to_csv('result.txt', encoding='utf-8')
    print('-' * 30)
    print(df_summerize.shape)
    # agg.stats_cal(result)
    # agg.check_cf_in_data(result)
    df_enhanced.to_csv('df_enhanced.txt', encoding='utf-8')
    return en_tid_dict


def build_call_stack(tid_df, key):
    df_en = pd.DataFrame(columns=('e_name', 'e_type', 'entry_ts', 'exit_ts', 'file_func', 'func_func', 'loc_func',
                                  'tid', 'pid', 'caller_row', 'caller_file', 'caller_func', 'duration_func'))
    myStack = deque()
    for row in range(len(df)):
        caller_row = 0
        entry_ts = 0
        exit_ts = 0
        entry_row = 0
        if df["e_type"].iloc[row] == 'en':
            if myStack:
                caller_row = myStack[-1]
            else:
                caller_row = 0
            myStack.append(row)
            entry_ts = df["cur_ts"].iloc[row]
            df_en.loc[row] = (df["e_name"].iloc[row], df["e_type"].iloc[row], entry_ts, "null",
                              df["file_func"].iloc[row], df["func_func"].iloc[row],
                              df["loc_func"].iloc[row], df["tid"].iloc[row], df["pid"].iloc[row], caller_row,
                              df["file_func"].iloc[caller_row], df["func_func"].iloc[caller_row], 0)
        elif df["e_type"].iloc[row] == "ex":
            if myStack:
                entry_row = myStack[-1]
                entry_ts = df["cur_ts"].iloc[entry_row]
                myStack.pop()
                exit_ts = df["cur_ts"].iloc[row]
                df_en['exit_ts'].loc[entry_row] = exit_ts
                duration = abs(int(exit_ts) - int(entry_ts))
                df_en['duration_func'].loc[entry_row] = duration
                caller_row = df_en['caller_row'].loc[entry_row]
            else:
                print("uneven events exception!")
            df_en.loc[row] = (df["e_name"].iloc[row], df["e_type"].iloc[row], entry_ts, exit_ts,
                              df["file_func"].iloc[row], df["func_func"].iloc[row],
                              df["loc_func"].iloc[row], df["tid"].iloc[row], df["pid"].iloc[row],
                              caller_row, df["file_func"].iloc[caller_row],
                              df["func_func"].iloc[caller_row], duration)
    str_df = str(key) + '.txt'
    df_en.to_csv(str_df, encoding='utf-8')
    return df_en

def build_graph(callstack_df, key):
    e_list = []
    graph_list = []
    m_list = []
    df_duration = pd.DataFrame(columns=('caller', 'callee', 'edge', 'duration'), dtype=int)
    graph = nx.MultiDiGraph()
    # we need to make a DAG so that each edge contains caller and callee nodes and the number
    # of repetitions of this call, which is named weight of the edge in this case
    for row in range(len(enhanced_df)):
        if enhanced_df["e_type"].iloc[row] == "ex":
            callee_string= str(enhanced_df["func_func"].iloc[row])
            caller_row = enhanced_df["caller_row"].iloc[row]
            caller_string = str(enhanced_df["func_func"].iloc[caller_row])
            caller_duration = int(enhanced_df['duration_func'].iloc[caller_row])
            dict = {'caller': caller_string, 'callee': callee_string, 'edge': (caller_string, callee_string),
                    'duration': caller_duration}
            df_duration = df_duration.append(dict, ignore_index=True)
            m_list.append((caller_string, callee_string))
            #if caller_string != callee_string:
            e_list.append((caller_string, callee_string))
            graph_list.append((caller_string, callee_string, caller_duration))
    print("test the df!")
    print("\n\n Duration is:")
    df_duration = df_duration.groupby(["caller", "callee"]) \
        .agg({'edge': 'size', 'duration': 'mean'}) \
        .rename(columns={'edge': 'count', 'duration': 'mean_duration'}) \
        .reset_index()

    print(df_duration)
    str_result = str(key) + '.csv'
    df_duration.to_csv(str_result)

    print("Size of graph is:")
    print(len(df_duration))

    graph = nx.from_pandas_edgelist(df_duration, source='caller', target='callee', edge_attr=['count', 'mean_duration'],
                                    create_using=nx.DiGraph)
    node_list= enhanced_df["func_func"].tolist()
    node_list = list(dict.fromkeys(node_list))
    graph.add_nodes_from(node_list)
    if nx.is_directed_acyclic_graph(graph) == True:
        print("Generated a DAG!")
    print("\n")
    nx.draw_networkx(graph, arrows=True)
    plt.savefig(str(key) + ".png", format="PNG")
    # clearing the current plot
    plt.clf()
    return  graph

def find_count_duration_path(graph, key, root):
    # def find_count_duration_path(graph, key):
    print("TESTING***************************************************************************")
    path = []
    roots = (v for v, d in graph.in_degree() if d == 0)
    # print(root)
    leaves = [v for v, d in graph.out_degree() if d == 0]
    leaves = list(leaves)
    print(root)
    print(leaves)
    path = nx.all_simple_paths(graph, root, leaves)
    print(path)
    # path = nx.all_simple_paths(graph, 'main', leaves)
    path = list(path)

    path_count = []  # <----- List to store all path's counts
    path_duration = []  # <----- List to store all path's duration
    for i in range(len(path)):
        total_count = 0
        total_duration = 0
        for j in range(len((path[i])) - 1):
            source, target = path[i][j], path[i][j + 1]
            edge = graph[source][target]
            count = edge['count']  # <--- Get the weight
            duration = edge['mean_duration']  # <--- Get the weight
            total_count += count
            total_duration += duration
        path_count.append(total_count)  # Append to the list
        path_duration.append(total_duration)  # Append to the list

    df_summerize = pd.DataFrame(list(zip(path, path_count, path_duration)),
                                columns=['path', 'count', 'duration'])
    print(df_summerize)

    str_paths = []
    str_list = ""
    for i in range(len(path)):
        path_list = path[i]
        str_list = ""
        for j in range(len(path_list)):
            str_list = str_list + str(path_list[j])
        str_paths.append(str_list)
        print("paths")
        print(str_paths)
    df_summerize['str_path'] = str_paths

    str_df = str(key) + 'PathSummary.txt'
    df_summerize.to_csv(str_df, encoding='utf-8')
    print("path count:")
    print(path_count)
    print("path duration:")
    print(path_duration)
    return df_summerize

def cal_wf_stats():

    return

def cal_func_stats(df):
    df = pd.DataFrame(columns=('e_name', 'e_type', 'entry_ts', 'exit_ts', 'file_func', 'func_func', 'loc_func',
                                        'tid', 'pid', 'caller_row', 'caller_file', 'caller_func', 'duration_func'))
    tids = trace_df.tid.unique()
    tid_dict = {elem: pd.DataFrame() for elem in tids}
    en_tid_dict = {elem: pd.DataFrame() for elem in tids}

    for key in (tid_dict.keys()):
        tid_dict[key] = trace_df[:][trace_df.tid == key]
        df_tid = tid_dict[key]
        df_en = build_call_stack(df_tid, key)
        en_tid_dict[key] = df_en
        dfs = [df, df_en]
        df = pd.concat(dfs)

    df_sorted=  df.sort_values('entry_ts')
    trace_ts = df_sorted.iloc[0]['entry_ts']
    df_func_stats = pd.DataFrame(columns=('trace_ts', 'e_name', 'func_dur_mean', 'func_dur_max', 'func_freq'))

    df_func= df.drop(columns=["e_type", "entry_ts", "exit_ts", "file_func", "loc_func", "tid", "pid", "caller_row", "caller_file", "caller_func"])
    df_func['freq'] = 1
    df_func_stats['trace_ts'] = trace_ts

    df_func_stats= df_func.groupby('e_name').agg(
        func_dur_mean=pd.NamedAgg(column="duration_func", aggfunc="mean"),
        func_dur_max=pd.NamedAgg(column="duration_func", aggfunc="max"),
        func_freq=pd.NamedAgg(column="freq", aggfunc="sum"))

    df_func_stats = df_func_stats.drop_duplicates(keep='first')
    return df_func_stats


def agg_trace_func():

def agg_trace_wf():
    return

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)
