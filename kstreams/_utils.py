#!/usr/bin/env python3
# coding: utf-8

from collections import namedtuple

import pandas as pd

SongInfo = namedtuple('SongInfo', ['id', 'title', 'artist', 'release_date'])


def interpolate(s):
    start = s.index[0].ceil('h')
    end = s.index[-1]
    new_index = pd.date_range(start, end, freq='1h')
    # create a mask to select the missing values from the input
    mask = s.reindex(new_index, method='bfill', tolerance='1h').isnull()
    interp = s.asfreq('s').interpolate().reindex(new_index)
    interp[mask] = None
    return interp


__all__ = []
