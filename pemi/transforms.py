import math
import numpy as np


def isblank(value):
    try:
        return math.isnan(value)
    except TypeError:
        blank = [np.nan, np.datetime64("NaT"), [], {}, None, '']
        return value in blank


def concatenate(delimiter=''):
    def _concatenate(row):
        return delimiter.join(row)

    return _concatenate


def nvl(default=''):
    def _nvl(row):
        return next((v for v in row if not isblank(v)), default)

    return _nvl
