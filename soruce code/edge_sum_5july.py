
import bt2
import sys
import pandas as pd
import networkx as nx
from collections import deque
import os
import math
import statsmodels.formula.api as smf
from matplotlib import pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import statsmodels.api as sm

directory = sys.argv[1]

def init(directory):
    edge_sum = pd.DataFrame(columns=('edge', 'source', 'target', 'entry_ts', 'duration', 'exit_ts'))
    # df_sum = pd.DataFrame(columns=('trace_ts', 'path', 'duration', 'str_path'))
    for filepath in os.listdir(directory):
        path = directory + filepath
        print(path)
        df = iterate_traces_to_df(path)
        if (len(df) != 0):
            # df_path_stats, edge_final = build_enhanced_df_tid(df)
            edge_final = build_enhanced_df_tid(df)
            # dfs = [df_sum, df_path_stats]
            edge_dfs = [edge_sum, edge_final]
            # df_sum = pd.concat(dfs)
            edge_sum = pd.concat(edge_dfs)
    # df_sum.to_csv('sum_path.txt', encoding='utf-8')
    edge_sum.to_csv('edge_sum.txt', encoding='utf-8')


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
            # cur_ts = event.timestamp
            df.loc[msg] = [event.name, event['event_type_field'], cur_ts, event['file_name_field'],
                           event['func_name_field'],
                           event['loc_field'], event['vtid'], event['vpid']]
    return df


def build_enhanced_df_tid(trace_df):
    df_enhanced = pd.DataFrame(columns=('e_name', 'e_type', 'entry_ts', 'exit_ts', 'file_func', 'func_func', 'loc_func',
                                        'tid', 'pid', 'caller_row', 'caller_file', 'caller_func', 'duration_func'))
    frames = []
    graphs_edge = []
    graphs = []
    tids = trace_df.tid.unique()
    tid_dict = {elem: pd.DataFrame() for elem in tids}
    en_tid_dict = {elem: pd.DataFrame() for elem in tids}

    for key in tid_dict.keys():
        tid_dict[key] = trace_df[:][trace_df.tid == key]
        df1 = tid_dict[key]
        df_en = build_call_stack(df1, key)
        en_tid_dict[key] = df_en
        dfs = [df_enhanced, df_en]
        df_enhanced = pd.concat(dfs)
        df_sorted = df_en.sort_values('entry_ts')
        root = df_sorted.iloc[0]['e_name']
        trace_ts = df_sorted.iloc[0]['entry_ts']
        df_graph, df_edge, graph = build_DAG(df_en, key)
        # find_paths(graph)
        # df_summerize = find_count_duration_path(graph, key, root)
        # frames.append(df_summerize)
        graphs.append(df_graph)
        graphs_edge.append(df_edge)
        # print(frames)
    # result = pd.concat(frames)
    graph_final = pd.concat(graphs)
    edge_final = pd.concat(graphs_edge)
    edge_final["edge"] = edge_final['source'] + edge_final['target']
    trace_ts = df_sorted.iloc[0]['entry_ts']
    # result['trace_ts'] = trace_ts
    # result.to_csv(str(trace_ts) + 'result.txt', encoding='utf-8')
    #graph_final.to_csv("graph_final", encoding='utf-8')
    #edge_final.to_csv("edge_final", encoding='utf-8')
    return edge_final


def build_call_stack(df, key):
    df_en = pd.DataFrame(columns=('e_name', 'e_type', 'entry_ts', 'exit_ts', 'file_func', 'func_func', 'loc_func',
                                  'tid', 'pid', 'caller_row', 'caller_file', 'caller_func', 'duration_func'))
    myStack = deque()
    for row in range(len(df)):
        caller_row = 0
        entry_ts = 0
        exit_ts = 0
        entry_row = 0
        duration = 0
        if df["e_type"].iloc[row] == 'en':
            if myStack:
                caller_row = myStack[-1]
            else:
                caller_row = 0
            myStack.append(row)
            entry_ts = df["cur_ts"].iloc[row]
            df_en.loc[row] = (df["e_name"].iloc[row], df["e_type"].iloc[row], entry_ts, exit_ts,
                              df["file_func"].iloc[row], df["func_func"].iloc[row],
                              df["loc_func"].iloc[row], df["tid"].iloc[row], df["pid"].iloc[row], caller_row,
                              df["file_func"].iloc[caller_row], df["func_func"].iloc[caller_row], duration)
        elif df["e_type"].iloc[row] == "ex":
            if myStack:
                entry_row = myStack[-1]
                entry_ts = df["cur_ts"].iloc[entry_row]
                exit_ts = df["cur_ts"].iloc[row]
                caller_row = df_en['caller_row'].loc[entry_row]
                if caller_row is None:
                    caller_row = row
                df_en.at[entry_row, 'exit_ts'] = exit_ts
                duration = exit_ts - entry_ts
                df_en.at[entry_row, 'duration_func'] = duration
                df_en.loc[row] = (df["e_name"].iloc[row], df["e_type"].iloc[row], entry_ts, exit_ts,
                                  df["file_func"].iloc[row], df["func_func"].iloc[row],
                                  df["loc_func"].iloc[row], df["tid"].iloc[row], df["pid"].iloc[row],
                                  caller_row, df["file_func"].iloc[caller_row],
                                  df["func_func"].iloc[caller_row], duration)
                myStack.pop()
            else:
                print("uneven events exception!")
    return df_en


def build_DAG(enhanced_df, key):
    e_list = []
    graph_list = []
    m_list = []
    dict = {}
    df_duration = pd.DataFrame(columns=('caller', 'callee', 'edge', 'duration', 'entry_ts', 'exit_ts'), dtype=int)
    graph = nx.MultiDiGraph()
    # we need to make a DAG so that each edge contains caller and callee nodes and the number
    # of repetitions of this call, which is named weight of the edge in this case
    for row in range((len(enhanced_df) - 1)):
        if enhanced_df["e_type"].iloc[row] == "ex":
            callee_string = str(enhanced_df["e_name"].iloc[row])
            if row in enhanced_df.index:
                caller_row = int(enhanced_df["caller_row"].loc[row])
                caller_string = str(enhanced_df["e_name"].iloc[caller_row])
                caller_duration = int(enhanced_df['duration_func'].iloc[caller_row])
                edge_entry_ts = int(enhanced_df['entry_ts'].iloc[row])
                edge_exit_ts = int(enhanced_df['exit_ts'].iloc[row])
                dict = {'caller': caller_string, 'callee': callee_string, 'edge': (caller_string, callee_string),
                        'duration': caller_duration, 'entry_ts': edge_entry_ts, 'exit_ts': edge_exit_ts}
                df_duration = df_duration.append(dict, ignore_index=True)
                m_list.append((caller_string, callee_string))
                e_list.append((caller_string, callee_string))
                graph_list.append((caller_string, callee_string, caller_duration, edge_entry_ts, edge_exit_ts))

    # print("Size of graph is:")
    # print(len(df_duration))

    graph = nx.from_pandas_edgelist(df_duration, source='caller', target='callee',
                                    edge_attr=['duration', 'entry_ts', 'exit_ts'],
                                    create_using=nx.DiGraph)
    node_list = enhanced_df["e_name"].tolist()
    node_list = list(dict.fromkeys(node_list))
    graph.add_nodes_from(node_list)
    if nx.is_directed_acyclic_graph(graph):
        print("Generated a DAG!")
    df_graph = nx.to_pandas_adjacency(graph)
    df_edge = nx.to_pandas_edgelist(graph)
    return df_graph, df_edge, graph


def find_count_duration_path(graph, key, root):
    path = []
    roots = [v for v, d in graph.out_degree() if d != 0]
    leaves = [v for v, d in graph.out_degree() if d == 0]
    roots = list(roots)
    leaves = list(leaves)

    for n in range(len(roots)):
        pathn = nx.all_simple_paths(graph, roots[n], leaves)
        pathn = list(pathn)
        path = path + pathn

    # path = nx.all_simple_paths(graph, roots, leaves)
    path = list(path)

    path_duration = []  # <----- List to store all path's duration
    path_entry_ts = []
    path_exit_ts = []
    for i in range(len(path)):
        total_duration = 0
        entry_ts = 0
        exit_ts = 0
        for j in range(len(path[i]) - 1):
            source, target = path[i][j], path[i][j + 1]
            edge = graph[source][target]
            duration = edge['duration']  # <--- Get the weight
            exit_ts = edge['exit_ts']
            if total_duration == 0:
                entry_ts = edge['entry_ts']
            total_duration += duration

        path_duration.append(total_duration)  # Append to the list
        path_entry_ts.append(entry_ts)
        path_exit_ts.append(exit_ts)

    df_summerize = pd.DataFrame(list(zip(path, path_duration, path_entry_ts, path_exit_ts)),
                                columns=['path', 'duration', 'entry_ts', 'exit_ts'])

    str_paths = []
    for i in range(len(path)):
        path_list = path[i]
        str_list = ""
        for j in range(len(path_list)):
            str_list = str_list + str(path_list[j])
        str_paths.append(str_list)
    df_summerize['str_path'] = str_paths
    return df_summerize

init(directory)