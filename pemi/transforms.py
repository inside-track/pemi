import pandas as pd

# TODO: concatenate, prefix, postfix, nvl, ifblank, blankif

def validate_no_null(field):
    def _validate(value):
        if value == field.null:
            raise ValueError("null is not allowed for field '{}'".format(field.name))
        return value
    return _validate

def isblank(value):
    return value is not False and (value is None or pd.isnull(value) or not value)

def concatenate(delimiter=''):
    def _concatenate(row):
        return delimiter.join(row)
    return _concatenate
