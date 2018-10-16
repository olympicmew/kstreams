#!/usr/bin/env python3
# coding: utf-8

import numpy as np
import pandas as pd


def interpolate(s):
    start = s.index[0].ceil('h')
    end = s.index[-1]
    new_index = pd.date_range(start, end, freq='1h')
    # create a mask to select the missing values from the input
    mask = s.reindex(new_index, method='bfill', tolerance='1h').isnull()
    interp = pd.concat([s, pd.DataFrame(np.nan, index=new_index,
                                        columns=s.columns)]).sort_index()
    interp = interp.interpolate(method='index').reindex(new_index)
    interp[mask] = None
    return interp


__all__ = []
