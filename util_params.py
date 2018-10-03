"""
    Auxiliary function used in the three tasks.
"""


def param_numeric_int(str):
    if str and str.isdigit():
        return int(str)
    else:
        return None
