import pandas as pd

def isblank(value):
    return (
        value is not False
        and value != 0
        and value != float(0)
        and (value is None or pd.isnull(value) or not value)
    )

def concatenate(delimiter=''):
    def _concatenate(row):
        return delimiter.join(row)
    return _concatenate

def nvl(default=''):
    def _nvl(row):
        return next((v for v in row if not isblank(v)), default)
    return _nvl
