from scipy.stats import norm, t, sem
import numpy as np
import pandas as pd

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