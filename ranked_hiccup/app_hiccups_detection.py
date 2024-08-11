"""
goal: application hiccups detection

test data gathering method:
same as perf-Problem
run a heavy Load test


input:
1. a dataset D consisting of each thread's execution paths  (path duration times series)/ (func duration times series) --> so for each path dataset D= path time series (resp_time, time_stamp)
2. threshold T that shows max response time acceptable for the path (this shows the performance requirements that are checked by a threshold)
3. maximum allowed proportion that the cumulative hiccup time may cover of the experiment duration (CH)

output: set of paths/funcs that are hiccup candidates, hiccup(H, start_time_stamp, end_time_stamp)

detection strategy:
1. assign threshold based on perf_prob_detection and identify hiccups H
2. calculate commulative duration of hiccups CH based on (D, H, experiment_duration)
3. rank the paths that are candidates of hiccup-problem based on the commulative probability CH (if it does not exceed a threshold and is small and repetitive enough)

bucket strategy: divide the response time series into buckets with a fixed, time-based width
if any executions in the bucket violate perf-req. that execution with start_time_stamp of the bucket is set as hiccup_start_time and it ends when a bucket meats perf_req. or dataset is finisihed. when a hiccup ends it is
added to the dataset of hiccups and hiccup flag is set to 0 so that algorithm can again search for new hiccups

bucket value calculation:
calculate mean time between two request (occurance of func or path) and consider a bucket that is sized between 10< <50 occurances (test with 10, 20, 50)

steps:
1. trace the services related to the test load and gather duration of each path
2. test if resp.time exceeds threshold (threshold is calculated in perf_problem_detect huristic)
3. calculate H which is the set of hiccups for the optimal threshold of all paths
4. calculate CH
4. keep CH and size of dataset (freq. of that path) for each path to rank
5. rank paths that are candidates of problem

algorithm:
filter based on path/func
calculate moving percentile --> M
in each percentile check if response time> threshold --> if yes, start hiccup till res.time < threshold and save the hiccup in the hiccups story and continue search for other hiccups
for each hiccup identified check how much is its duration, if larger than a threshold it can't be considered hiccups

"""


import bt2
import sys
import pandas as pd
import networkx as nx
from collections import deque
import os
import math

directory = sys.argv[1]

#df_func_sum = pd.DataFrame(columns=('trace_ts', 'e_name', 'func_dur_mean' , 'func_dur_max' , 'func_freq'))
df_rank_funcs= pd.DataFrame(columns=('path_rank', 'path_name', 'path_score'))


def init(directory):
    edge_sum = pd.DataFrame(columns=('edge', 'source','target','entry_ts','duration','exit_ts'))
    #df_sum = pd.DataFrame(columns=('trace_ts', 'path', 'duration', 'str_path'))
    for filepath in os.listdir(directory):
        path = directory + filepath
        print(path)
        df = iterate_traces_to_df(path)
        if (len(df) != 0):
            #df_path_stats, edge_final = build_enhanced_df_tid(df)
            edge_final = build_enhanced_df_tid(df)
            #dfs = [df_sum, df_path_stats]
            edge_dfs = [edge_sum, edge_final]
            #df_sum = pd.concat(dfs)
            edge_sum = pd.concat(edge_dfs)
    #df_sum.to_csv('sum_path.txt', encoding='utf-8')
    edge_sum.to_csv('edge_sum.txt', encoding='utf-8')

    thresholds = [1000, 2419243, 3000000, 4000000, 5000000, 10000000]
    for i in range(len(thresholds)):
        t = int(thresholds[i])
        df_th = check_threshold(edge_sum, t)
        ranked_df = cal_commulative_probability_edge(df_th)
        str_df = str(i) + "ranked_commulative_probability"
        ranked_df.to_csv(str_df, encoding='utf-8')
        #hiccups, df_sorted= check_hiccup(df_sum,t)
        hiccups_edge, df_sorted_edge = check_hiccup_edge(edge_sum, t)
        ranked_hiccup_cummulative_probability = cal_commulative_probability_edge_hiccup(hiccups_edge)
        str_h = str(i) + "hiccup"
        hiccups_edge.to_csv(str_h, encoding='utf-8')
        str_h = str(i) + "ranked_hiccup_cummulative_probability"
        ranked_hiccup_cummulative_probability.to_csv(str_h, encoding='utf-8')



        #df_hiccup= check_hiccup(df, t)
        #df_valid_hiccup= validate_hiccup(df_hiccup,t)
        #str_df2 = str(i) + "valid hiccups"
        #df_valid_hiccup.to_csv(str_df, encoding='utf-8')


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
            #cur_ts = event.timestamp
            df.loc[msg] = [event.name, event['event_type_field'], cur_ts, event['file_name_field'],
                           event['func_name_field'],
                           event['loc_field'], event['vtid'], event['vpid']]
    return df


def build_enhanced_df_tid(trace_df):
    df_enhanced = pd.DataFrame(columns=('e_name', 'e_type', 'entry_ts', 'exit_ts', 'file_func', 'func_func', 'loc_func',
                                        'tid', 'pid', 'caller_row', 'caller_file', 'caller_func', 'duration_func'))
    frames = []
    graphs_edge=[]
    graphs= []
    tids = trace_df.tid.unique()
    print("tids")
    print(tids)
    tid_dict = {elem: pd.DataFrame() for elem in tids}
    en_tid_dict = {elem: pd.DataFrame() for elem in tids}

    for key in tid_dict.keys():
        tid_dict[key] = trace_df[:][trace_df.tid == key]
        df1 = tid_dict[key]
        df_en = build_call_stack(df1, key)
        en_tid_dict[key] = df_en
        dfs = [df_enhanced, df_en]
        df_enhanced = pd.concat(dfs)
        df_sorted= df_en.sort_values('entry_ts')
        root= df_sorted.iloc[0]['e_name']
        trace_ts = df_sorted.iloc[0]['entry_ts']
        df_graph, df_edge, graph = build_DAG(df_en, key)
        # find_paths(graph)
        #df_summerize = find_count_duration_path(graph, key, root)
        #frames.append(df_summerize)
        graphs.append(df_graph)
        graphs_edge.append(df_edge)
        #print(frames)
    #result = pd.concat(frames)
    graph_final = pd.concat(graphs)
    edge_final = pd.concat(graphs_edge)
    edge_final["edge"] = edge_final['source'] + edge_final['target']
    trace_ts = df_sorted.iloc[0]['entry_ts']
    #result['trace_ts'] = trace_ts
    #result.to_csv(str(trace_ts) + 'result.txt', encoding='utf-8')
    graph_final.to_csv("graph_final", encoding='utf-8')
    edge_final.to_csv("edge_final", encoding='utf-8')
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
        duration= 0
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
                    caller_row= row
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
    dict={}
    df_duration = pd.DataFrame(columns=('caller', 'callee', 'edge', 'duration', 'entry_ts','exit_ts'), dtype=int)
    graph = nx.MultiDiGraph()
    # we need to make a DAG so that each edge contains caller and callee nodes and the number
    # of repetitions of this call, which is named weight of the edge in this case
    for row in range((len(enhanced_df)-1)):
        if enhanced_df["e_type"].iloc[row] == "ex":
            callee_string= str(enhanced_df["e_name"].iloc[row])
            if row in enhanced_df.index:
                caller_row = int(enhanced_df["caller_row"].loc[row])
                caller_string = str(enhanced_df["e_name"].iloc[caller_row])
                caller_duration = int(enhanced_df['duration_func'].iloc[caller_row])
                edge_entry_ts= int(enhanced_df['entry_ts'].iloc[row])
                edge_exit_ts= int(enhanced_df['exit_ts'].iloc[row])
                dict = {'caller': caller_string, 'callee': callee_string, 'edge': (caller_string, callee_string),
                        'duration': caller_duration, 'entry_ts': edge_entry_ts, 'exit_ts': edge_exit_ts}
                df_duration = df_duration.append(dict, ignore_index=True)
                m_list.append((caller_string, callee_string))
                e_list.append((caller_string, callee_string))
                graph_list.append((caller_string, callee_string, caller_duration, edge_entry_ts, edge_exit_ts))

    #print("Size of graph is:")
    #print(len(df_duration))

    graph = nx.from_pandas_edgelist(df_duration, source='caller', target='callee', edge_attr=['duration', 'entry_ts', 'exit_ts'],
                                    create_using=nx.DiGraph)
    node_list= enhanced_df["e_name"].tolist()
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
    print("roots")
    print(roots)
    leaves = [v for v, d in graph.out_degree() if d == 0]
    print(leaves)
    roots= list(roots)
    leaves= list(leaves)

    for n in range(len(roots)):
        pathn = nx.all_simple_paths(graph, roots[n], leaves)
        pathn = list(pathn)
        path= path+ pathn

    #path = nx.all_simple_paths(graph, roots, leaves)
    path= list(path)

    path_duration= []  # <----- List to store all path's duration
    path_entry_ts=[]
    path_exit_ts = []
    for i in range(len(path)):
        total_duration = 0
        entry_ts=0
        exit_ts=0
        for j in range(len(path[i])-1):
            source, target = path[i][j], path[i][j + 1]
            edge = graph[source][target]
            duration = edge['duration']  # <--- Get the weight
            exit_ts = edge['exit_ts']
            if total_duration == 0:
                entry_ts= edge['entry_ts']
            total_duration += duration

        path_duration.append(total_duration)  # Append to the list
        path_entry_ts.append(entry_ts)
        path_exit_ts.append(exit_ts)

    df_summerize = pd.DataFrame(list(zip(path, path_duration, path_entry_ts, path_exit_ts)),
                      columns=['path', 'duration', 'entry_ts', 'exit_ts'])

    str_paths=[]
    for i in range(len(path)):
        path_list= path[i]
        str_list = ""
        for j in range(len(path_list)):
            str_list= str_list+ str(path_list[j])
        str_paths.append(str_list)
        print(str_paths)
    df_summerize['str_path'] = str_paths
    return df_summerize


def check_threshold(df, t):
    threshold_flag=[]
    print(t)
    for row in range(len(df)):
        if (int(df['duration'].iloc[row])) > t:
            threshold_flag.append(1)
        else:
            threshold_flag.append(0)
    df['threshold_flag'] = threshold_flag

    df.to_csv("df_th", encoding='utf-8')
    return df

def cal_commulative_probability(df_th):
    df_has_problem = df_th.groupby(['str_path']).apply(lambda x: x[x['threshold_flag'] == 1]['duration'].count())
    df_whole = df_th.groupby(['str_path']).agg(
        count_whole=pd.NamedAgg(column="duration", aggfunc="count"))
    df_whole['has_problem'] = df_has_problem[1]
    df_whole['commulative_probability'] = df_whole['has_problem'] / df_whole['count_whole']

    df_has_problem.to_csv("df_has_problem", encoding='utf-8')
    df_whole.to_csv("df_whole", encoding='utf-8')

    ranked_df = df_whole.sort_values('commulative_probability')
    return ranked_df

def cal_commulative_probability_edge(df_th):
    df_has_problem = df_th.groupby(['edge']).apply(lambda x: x[x['threshold_flag'] == 1]['duration'].count())
    df_whole = df_th.groupby(['edge']).agg(
        count_whole=pd.NamedAgg(column="duration", aggfunc="count"))
    df_whole['has_problem'] = df_has_problem[1]
    df_whole['commulative_probability'] = df_whole['has_problem'] / df_whole['count_whole']

    df_has_problem.to_csv("df_has_problem", encoding='utf-8')
    df_whole.to_csv("df_whole", encoding='utf-8')

    ranked_df = df_whole.sort_values('commulative_probability')
    return ranked_df
"""
filter based on path/func
calculate moving percentile --> M
in each percentile check if response time> threshold --> if yes, start hiccup till res.time < threshold and save the hiccup in the hiccups story and continue search for other hiccups
for each hiccup identified check how much is its duration, if larger than a threshold it can't be considered hiccups


"""
"""
def check_hiccup(df, t):
    df_sorted = df.sort_values(['str_path', 'entry_ts'])
    df_sorted['diff'] = abs(df_sorted.entry_ts.diff()) #calculate occurance distance of the specific path
    df_path_occ_diff = df_sorted.groupby(['str_path']).agg(occ_diff = pd.NamedAgg(column="diff", aggfunc="mean"))
    #ranges_for_occurance= {10, 25, 50}
    df_sorted=df.sort_values(['str_path','entry_ts'])
    moving_percentile = []
    for row in range(0, len(df_path_occ_diff)):
        moving_percentile.append((df_path_occ_diff['occ_diff'][row]) * 10)
    df_path_occ_diff["moving_percentile"] = moving_percentile
    df = pd.merge(df_sorted, df_path_occ_diff, on="str_path")

    print(df)

    # create unique list of paths
    UniquePaths = df.str_path.unique()

    # create a data frame dictionary to store your data frames
    DataFrameDict = {elem: pd.DataFrame() for elem in UniquePaths}
    HiccupDict= {elem: pd.DataFrame() for elem in UniquePaths}

    for key in DataFrameDict.keys():
        DataFrameDict[key] = df[:][df.str_path == key]
        df_moving_percentile= DataFrameDict[key]
        time_step = df['moving_percentile'][df.str_path == key]
        time_step= list(time_step)
        start_ts = df_moving_percentile['entry_ts'].iloc[0]
        end_ts = df_moving_percentile['exit_ts'].iloc[-1]
        num_of_windows= math.ceil(abs((end_ts - start_ts) / time_step[0]))
        print(start_ts)
        print(end_ts)
        print(num_of_windows)
        ts = start_ts
        r= 0
        threshold_flag = []

        df_hiccup = pd.DataFrame(columns=('str_path', 'start', 'duration', 'end'))
        for k in range(0, int(num_of_windows)):
            window_ts = (time_step * k) + start_ts
            next_window_ts = (time_step[0] * (k+1)) + start_ts
            hiccupflag= 0
            hiccup_index = 0
            while ts < next_window_ts:
                if(r<num_of_windows):
                    if (int(df_moving_percentile['duration'].iloc[r])) > t:
                        threshold_flag.append(1)
                        if hiccupflag == 0:
                            hiccupflag = 1
                            print("r")
                            print(r)
                            hiccup_start = df_moving_percentile['entry_ts'].iloc[r]
                            hiccup_str_path = df_moving_percentile['str_path'].iloc[r]

                            #df_hiccup.at[hiccup_index, 'start'] = df_moving_percentile['entry_ts'].iloc[r]
                            #df_hiccup.at[hiccup_index, 'str_path'] = df_moving_percentile['str_path'].iloc[r]
                            hiccup_end = df_moving_percentile['exit_ts'].iloc[r]
                            hiccup_dur = hiccup_end - hiccup_start

                    else:
                        threshold_flag.append(0)
                        if hiccupflag == 1:
                            hiccupflag = 0
                            hiccup_end = df_moving_percentile['exit_ts'].iloc[r]
                            hiccup_dur = hiccup_end - hiccup_start
                            #hiccup_dur = df_hiccup['end'].iloc[hiccup_index] - \
                                                                df_hiccup['start'].iloc[hiccup_index]
                            #df_hiccup.loc[hiccup_index] = (hiccup_str_path, hiccup_start, hiccup_end, hiccup_dur)
                    new_row = [hiccup_str_path, hiccup_start, hiccup_end, hiccup_dur]
                    df_hiccup.append(pd.Series(new_row), ignore_index=True)
                            hiccup_index = hiccup_index + 1
                    ts = df_moving_percentile['exit_ts'].iloc[r]
                    r = r + 1
                    print(df_hiccup)
            df_moving_percentile['threshold_flag'] = threshold_flag
            HiccupDict[key] = df_hiccup
        df_sorted = pd.concat(DataFrameDict.values(), ignore_index=True)
        df_sorted.to_csv("df_sorted", encoding='utf-8')
        hiccups = pd.concat(HiccupDict.values(), ignore_index=True)
        hiccups.to_csv("hiccups", encoding='utf-8')
    return hiccups, df_sorted
"""
def __hash__(self):
    return hash(self.stream)

def check_hiccup_edge(df2, t):
    df= df2.drop_duplicates()
    print("duplicated columns")
    print(df.duplicated(subset=['entry_ts']))
    df_sorted2 = df.sort_values(['edge'])
    df_sorted = df_sorted2.sort_values(['entry_ts'])
    df_sorted['diff'] = abs(df_sorted.entry_ts.diff()) #calculate occurance distance of the specific path
    df_sorted['diff']= pd.to_numeric(df_sorted['diff'])
    df_path_occ_diff = df_sorted.groupby(['edge']).agg(occ_diff = pd.NamedAgg(column="diff", aggfunc="mean"))
    #ranges_for_occurance= {10, 25, 50}
    #df_sorted=df.sort_values(['edge','entry_ts'])
    moving_percentile = []
    for row in range(0, len(df_path_occ_diff)):
        moving_percentile.append(math.floor((df_path_occ_diff['occ_diff'][row]) * 10))
    df_path_occ_diff["moving_percentile"] = moving_percentile
    df = pd.merge(df_sorted, df_path_occ_diff, on="edge")

    print(df)

    # create unique list of paths
    UniqueEdges = df.edge.unique()

    # create a data frame dictionary to store your data frames
    DataFrameDict = {elem: pd.DataFrame() for elem in UniqueEdges}
    HiccupDict= {elem: pd.DataFrame() for elem in UniqueEdges}
    window_sizes=[]
    for key in DataFrameDict.keys():
        DataFrameDict[key] = df[:][df.edge == key]
        df_moving_percentile= DataFrameDict[key]
        time_step = df['moving_percentile'][df.edge == key]
        time_step= list(time_step)
        window_size= time_step[0]
        window_sizes.append(window_size)
        start_ts = df_moving_percentile['entry_ts'].iloc[0]
        end_ts = df_moving_percentile['exit_ts'].iloc[-1]
        num_of_windows = math.ceil(abs((end_ts - start_ts) / window_size))
        print(start_ts)
        print(end_ts)
        print(num_of_windows)
        ts = start_ts
        #r= 0
        df_hiccup = pd.DataFrame(columns=('edge', 'start', 'end', 'duration', 'moving_percentile', 'window_size'))
        threshold_flag = []
        for k in range(0, int(num_of_windows)):
            window_ts = (window_size * k) + start_ts
            next_window_ts = window_size + window_ts
            hiccupflag= 0
            hiccup_index = 0
            #r = 0
            print(k)
            print(next_window_ts)
            hiccup_start_index = 0
            for r in range(len(df_moving_percentile)):
                if (int(df_moving_percentile['duration'].iloc[r])) > t:
                    print(ts)
                    threshold_flag.append(1)
                    print('threshold_flag.append(1)')
                    df_hiccup.at[hiccup_index, 'moving_percentile'] = k
                    df_hiccup.at[hiccup_index, 'window_size'] = window_size
                    ts = df_moving_percentile['exit_ts'].iloc[r]
                    if hiccupflag == 0:
                        hiccupflag = 1
                        print('hiccupflag = 1')
                        hiccup_start_index = r
                        df_hiccup.at[hiccup_index, 'start'] = df_moving_percentile['entry_ts'].iloc[hiccup_start_index]
                        df_hiccup.at[hiccup_index, 'edge'] = df_moving_percentile['edge'].iloc[hiccup_start_index]
                        df_hiccup.at[hiccup_index, 'end'] = df_moving_percentile['exit_ts'].iloc[hiccup_start_index]
                        df_hiccup.at[hiccup_index, 'duration'] = df_moving_percentile['exit_ts'].iloc[
                                                                     hiccup_start_index] - \
                                                                 df_moving_percentile['entry_ts'].iloc[
                                                                     hiccup_start_index]
                    elif hiccupflag == 1:
                        print('hiccupflag = 1 condition')
                        df_hiccup.at[hiccup_index, 'start'] = df_moving_percentile['entry_ts'].iloc[hiccup_start_index]
                        df_hiccup.at[hiccup_index, 'edge'] = df_moving_percentile['edge'].iloc[hiccup_start_index]
                        df_hiccup.at[hiccup_index, 'end'] = df_moving_percentile['exit_ts'].iloc[r]
                        df_hiccup.at[hiccup_index, 'duration'] = df_moving_percentile['exit_ts'].iloc[r] - \
                                                                 df_moving_percentile['entry_ts'].iloc[
                                                                     hiccup_start_index]
                else:
                    threshold_flag.append(0)
                    print('threshold_flag.append(0)')
                    if hiccupflag == 1:
                        print('threshold_flag.append(0) hiccupflag == 1')
                        hiccupflag = 0
                        df_hiccup.at[hiccup_index, 'start'] = df_moving_percentile['entry_ts'].iloc[hiccup_start_index]
                        df_hiccup.at[hiccup_index, 'edge'] = df_moving_percentile['edge'].iloc[hiccup_start_index]
                        df_hiccup.at[hiccup_index, 'end'] = df_moving_percentile['exit_ts'].iloc[r - 1]
                        df_hiccup.at[hiccup_index, 'duration'] = df_moving_percentile['exit_ts'].iloc[r - 1] - \
                                                                 df_moving_percentile['entry_ts'].iloc[
                                                                     hiccup_start_index]
                        hiccup_index = hiccup_index + 1

        print(df_hiccup)
        #df_moving_percentile['threshold_flag'] = threshold_flag
        HiccupDict[key] = df_hiccup
    df_sorted = pd.concat(DataFrameDict.values(), ignore_index=True)
    df_sorted.to_csv("df_sorted", encoding='utf-8')
    hiccups = pd.concat(HiccupDict.values(), ignore_index=True)
    hiccups.to_csv("hiccups", encoding='utf-8')
    return hiccups, df_sorted

def cal_commulative_probability_edge_hiccup(hiccups):
    df_edge_hiccups = hiccups.groupby(['edge']).agg({'duration':'sum', 'moving_percentile':'count', 'window_size':'first'})
    df_edge_hiccups['cummulative_probability'] = df_edge_hiccups['duration'] / (df_edge_hiccups['window_size']* df_edge_hiccups['moving_percentile'])
    df_edge_hiccups_ranked = df_edge_hiccups.sort_values(['cummulative_probability'])
    return df_edge_hiccups_ranked



init(directory)


