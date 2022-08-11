from scipy.stats import norm, t, sem
import numpy as np
import pandas as pd

output_path = "C:\\Users\\peaju\\THAI AIRWAYS INTERNATIONAL PUBLIC CO.,LTD\\DP - PC - Team 1 - Documents\\PC - Team 1\\2022\\python\\dash_board\\output\\"
data_path = "C:\\Users\\peaju\\THAI AIRWAYS INTERNATIONAL PUBLIC CO.,LTD\\DP - PC - Team 1 - Documents\\PC - Team 1\\2022\\python\\dash_board\\data\\"

def export_csv(df, name):
    df.to_csv(output_path + name +'.csv')

def export_json(df, name):
    df.to_json(output_path + name + '.json')

def load_merged_df():
    return pd.read_json(data_path + 'merged_data.json')

def cdf(series):
    """Return X, y for plot cdf"""
    total = len(series)
    step = 1/total
    x = np.sort(series)
    y = np.arange(step,1 + step, step)
    return x, y

def mean_confidence_interval(data, conf = 0.95):
    n = len(data)
    mean, se = data.mean(), sem(data)
    if n < 30:
        h = se * t.ppf((1 + conf) /2., n - 1)
    else:
        h = se * norm.ppf((1 + conf) /2.)
    return mean, mean - h, mean + h