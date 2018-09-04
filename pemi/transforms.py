import math
import numpy as np
import pandas as pd

def _isnan(value):
    try:
        return math.isnan(value)
    except TypeError:
        return None

def _isnat(value):
    try:
        return bool(np.isnat(value))
    except TypeError:
        return None

def isblank(value):
    isnan = _isnan(value)
    isnat = _isnat(value)

    if isnan is not None: return isnan
    if isnat is not None: return isnat

    blank = [np.nan, pd.NaT, [], {}, None, '']
    return value in blank


def concatenate(delimiter=''):
    def _concatenate(row):
        return delimiter.join(row)

    return _concatenate


def nvl(default=''):
    def _nvl(row):
        return next((v for v in row if not isblank(v)), default)

    return _nvl
