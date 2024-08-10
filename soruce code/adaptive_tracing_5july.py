import sys
import pandas as pd
import os
import math
from matplotlib import pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

directory = sys.argv[1]

def init(directory):
    edge_sum = pd.DataFrame(columns=('edge', 'source', 'target', 'entry_ts', 'duration', 'exit_ts'))
    for filepath in os.listdir(directory):
        path = directory + filepath
        edge_sum = pd.read_csv(path, sep=',', lineterminator='\n')

        edge_sum['entry_ts'] = pd.to_numeric(edge_sum['entry_ts'])
        edge_sum['exit_ts'] = pd.to_numeric(edge_sum['exit_ts'])
        edge_sum['duration'] = pd.to_numeric(edge_sum['duration'])

    linear_reg = linear_regression(edge_sum)
    linear_reg.to_csv("linear_reg.txt", encoding='utf-8')

    thresholds = [1000, 10000, 100000, 1000000, 2419243, 3000000, 4000000, 5000000, 10000000]
    for i in range(len(thresholds)):
        t = int(thresholds[i])
        df_th = check_threshold(edge_sum, t)
        ranked_df = cal_commulative_probability_edge(df_th)
        str_df = str(i) + "ranked_commulative_probability"
        ranked_df.to_csv(str_df, encoding='utf-8')


    check_hiccup_edge(edge_sum)


def check_threshold(df, t):
    threshold_flag = []
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

def __hash__(self):
    return hash(self.stream)


def check_hiccup_edge(df2):
    df = df2.drop_duplicates()
    df_sorted2 = df.sort_values(['edge'])
    df_sorted = df_sorted2.sort_values(['entry_ts'])
    df_sorted['diff'] = abs(df_sorted.entry_ts.diff())  # calculate occurance distance of the specific path
    df_sorted['diff'] = pd.to_numeric(df_sorted['diff'])
    df_sorted['duration'] = pd.to_numeric(df_sorted['duration'])
    df_path_occ_diff = df_sorted.groupby(['edge']).agg(occ_diff=pd.NamedAgg(column="diff", aggfunc="mean"),
                                                       mean_dur=pd.NamedAgg(column="duration", aggfunc="mean"))
    # ranges_for_occurance= {10, 25, 50}
    # df_sorted=df.sort_values(['edge','entry_ts'])
    moving_percentile = []
    thresholds = []
    for row in range(0, len(df_path_occ_diff)):
        moving_percentile.append(math.floor((df_path_occ_diff['occ_diff'][row]) * 10))
        thresholds.append(df_path_occ_diff['mean_dur'][row])
    df_path_occ_diff["moving_percentile"] = moving_percentile
    df_path_occ_diff["threshold"] = thresholds
    df = pd.merge(df_sorted, df_path_occ_diff, on="edge")
    # create unique list of paths
    UniqueEdges = df.edge.unique()

    # create a data frame dictionary to store your data frames
    DataFrameDict = {elem: pd.DataFrame() for elem in UniqueEdges}
    HiccupDict = {elem: pd.DataFrame() for elem in UniqueEdges}
    window_sizes = []
    for i in range(-4, 4):
        for key in DataFrameDict.keys():
            DataFrameDict[key] = df[:][df.edge == key]
            df_moving_percentile = DataFrameDict[key]
            time_step = df['moving_percentile'][df.edge == key]
            threshold = df['threshold'][df.edge == key]
            time_step = list(time_step)
            window_size = time_step[0]
            threshold = list(threshold)
            t_main = threshold[0]
            t = t_main * (10 ** i) + t_main
            window_sizes.append(window_size)
            start_ts = df_moving_percentile['entry_ts'].iloc[0]
            end_ts = df_moving_percentile['exit_ts'].iloc[-1]
            num_of_windows = math.ceil(abs((end_ts - start_ts) / window_size))
            ts = start_ts
            # r= 0
            df_hiccup = pd.DataFrame(columns=('edge', 'start', 'end', 'duration', 'moving_percentile', 'window_size'))
            threshold_flag = []
            df_edge_hiccups_ranked = pd.DataFrame(columns=('edge', 'cummulative_probability'))
            for k in range(0, int(num_of_windows)):
                window_ts = (window_size * k) + start_ts
                next_window_ts = window_size + window_ts
                hiccupflag = 0
                hiccup_index = 0
                # r = 0
                hiccup_start_index = 0
                for r in range(len(df_moving_percentile)):
                    if (int(df_moving_percentile['duration'].iloc[r])) > t:
                        threshold_flag.append(1)
                        df_hiccup.at[hiccup_index, 'moving_percentile'] = k
                        df_hiccup.at[hiccup_index, 'window_size'] = window_size
                        ts = df_moving_percentile['exit_ts'].iloc[r]
                        if hiccupflag == 0:
                            hiccupflag = 1
                            hiccup_start_index = r
                            df_hiccup.at[hiccup_index, 'start'] = df_moving_percentile['entry_ts'].iloc[
                                hiccup_start_index]
                            df_hiccup.at[hiccup_index, 'edge'] = df_moving_percentile['edge'].iloc[hiccup_start_index]
                            df_hiccup.at[hiccup_index, 'end'] = df_moving_percentile['exit_ts'].iloc[hiccup_start_index]
                            df_hiccup.at[hiccup_index, 'duration'] = df_moving_percentile['exit_ts'].iloc[
                                                                         hiccup_start_index] - \
                                                                     df_moving_percentile['entry_ts'].iloc[
                                                                         hiccup_start_index]
                        elif hiccupflag == 1:
                            df_hiccup.at[hiccup_index, 'start'] = df_moving_percentile['entry_ts'].iloc[
                                hiccup_start_index]
                            df_hiccup.at[hiccup_index, 'edge'] = df_moving_percentile['edge'].iloc[hiccup_start_index]
                            df_hiccup.at[hiccup_index, 'end'] = df_moving_percentile['exit_ts'].iloc[r]
                            df_hiccup.at[hiccup_index, 'duration'] = df_moving_percentile['exit_ts'].iloc[r] - \
                                                                     df_moving_percentile['entry_ts'].iloc[
                                                                         hiccup_start_index]
                    else:
                        threshold_flag.append(0)
                        if hiccupflag == 1:
                            hiccupflag = 0
                            df_hiccup.at[hiccup_index, 'start'] = df_moving_percentile['entry_ts'].iloc[
                                hiccup_start_index]
                            df_hiccup.at[hiccup_index, 'edge'] = df_moving_percentile['edge'].iloc[hiccup_start_index]
                            df_hiccup.at[hiccup_index, 'end'] = df_moving_percentile['exit_ts'].iloc[r - 1]
                            df_hiccup.at[hiccup_index, 'duration'] = df_moving_percentile['exit_ts'].iloc[r - 1] - \
                                                                     df_moving_percentile['entry_ts'].iloc[
                                                                         hiccup_start_index]
                            hiccup_index = hiccup_index + 1
            # df_moving_percentile['threshold_flag'] = threshold_flag
            HiccupDict[key] = df_hiccup
        df_sorted = pd.concat(DataFrameDict.values(), ignore_index=True)
        df_sorted.to_csv("df_sorted.txt", encoding='utf-8')
        hiccups = pd.concat(HiccupDict.values(), ignore_index=True)
        hiccups.to_csv("hiccups.txt", encoding='utf-8')
        ranked_hiccup_cummulative_probability = cal_commulative_probability_edge_hiccup(hiccups)
        k = i + 4
        str_h = str(k) + "hiccup"
        hiccups.to_csv(str_h, encoding='utf-8')
        str_c = str(k) + "ranked_hiccup_cummulative_probability"
        ranked_hiccup_cummulative_probability.to_csv(str_c, encoding='utf-8')
    return hiccups, df_sorted

def cal_commulative_probability_edge_hiccup(hiccups):
    df_edge_hiccups = hiccups.groupby(['edge']).agg(
        {'duration': 'sum', 'moving_percentile': 'count', 'window_size': 'first'})
    df_edge_hiccups['cummulative_probability'] = df_edge_hiccups['duration'] / (
                df_edge_hiccups['window_size'] * df_edge_hiccups['moving_percentile'])
    df_edge_hiccups_ranked = df_edge_hiccups.sort_values(['cummulative_probability'])
    return df_edge_hiccups_ranked


def linear_regression(df2):
    df = df2.drop_duplicates()
    df_grouped = pd.DataFrame(columns=('edge', 'duration', 'r_dur'))
    df_ramp_edge = pd.DataFrame(columns=('edge', 'slope', 'intercept', 'MSE', 'Root_mean_squared_error', 'R2_score'))
    time_window_size = 1000000000
    df_sorted = df.sort_values(['entry_ts'])
    df_sorted['duration'] = df_sorted['duration'].astype(float)
    trace_start_ts = df_sorted['entry_ts'].iloc[0]
    trace_end_ts = df_sorted['exit_ts'].iloc[-1]
    window_start_ts = trace_start_ts
    windows_end_ts = trace_start_ts + time_window_size
    num_of_windows = int(math.ceil(abs(trace_end_ts - window_start_ts) / time_window_size))
    #df_grouped = df_sorted.groupby(['edge'], as_index=False).agg(
    #   mean_dur=pd.NamedAgg(column="duration", aggfunc="mean"),
    #    frequency=pd.NamedAgg(column="edge", aggfunc="count"))
    for w in range(0, num_of_windows):
        df_filtered = df_sorted[(df_sorted['entry_ts'] >= window_start_ts) & (df_sorted['exit_ts'] < windows_end_ts)]
        # df_filtered_grouped = df_filtered.groupby(['edge'], as_index=False).agg(
        #    {'duration': 'mean', 'r_dur': 'count', 'source': 'first', 'target': 'first'})
        df_filtered_grouped = df_filtered.groupby(['edge'], as_index=False).agg(
            mean_dur=pd.NamedAgg(column="duration", aggfunc="mean"),
            frequency=pd.NamedAgg(column="edge", aggfunc="count"))
        filtered_grouped_dfs = [df_grouped, df_filtered_grouped]
        window_start_ts = windows_end_ts
        windows_end_ts = windows_end_ts + time_window_size
        df_grouped = pd.concat(filtered_grouped_dfs)
    # df_grouped= df_grouped2.dropna(axis=1, how="any", thresh=None, subset=None, inplace=False)
    df_grouped.to_csv("df_grouped", encoding='utf-8')
    UniqueEdges = df_grouped.edge.unique()

    for i in range(len(UniqueEdges)):
        df_edge = df_grouped[:][df_grouped.edge == UniqueEdges[i]]
        #mean_dur = df_grouped['mean_dur'][df_grouped.edge == UniqueEdges[i]]
        mean_dur = df_edge['mean_dur']
        x_list = list(mean_dur)
        frequency = df_edge['frequency']
        y_list = list(frequency)
        n = np.size(x_list)

        x = np.array(x_list)
        y = np.array(y_list)
        plt.plot(x, y, '*', color='blue')
        plt.xlabel('duration')
        plt.ylabel('frequency')

        str_name = str(UniqueEdges[i]) + "model.png"
        plt.savefig(str_name, format="PNG")
        plt.clf()

        regression_model = LinearRegression()
        x = x.reshape(-1, 1)
        y = y.reshape(-1, 1)
        # Fit the data(train the model)
        regression_model.fit(x, y)

        # Predict
        y_predicted = regression_model.predict(x)

        # model evaluation
        mse = mean_squared_error(y, y_predicted)

        rmse = np.sqrt(mean_squared_error(y, y_predicted))
        r2 = r2_score(y, y_predicted)

        df_ramp_edge.at[i, 'edge'] = UniqueEdges[i]
        df_ramp_edge.at[i, 'slope'] = regression_model.coef_
        df_ramp_edge.at[i, 'intercept'] = regression_model.intercept_
        df_ramp_edge.at[i, 'MSE'] = mse
        df_ramp_edge.at[i, 'Root_mean_squared_error'] = rmse
        df_ramp_edge.at[i, 'R2_score'] = r2

        plt.scatter(x, y, color='red')
        plt.plot(x, y_predicted, color='green')
        plt.xlabel('X')
        plt.ylabel('y')
        str_name = str(UniqueEdges[i]) + "model2.png"
        plt.savefig(str_name, format="PNG")
        plt.clf()
    return df_ramp_edge


init(directory)

