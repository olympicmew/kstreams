# -*- coding: utf-8 -*-

from collections import namedtuple
SongInfo = namedtuple('SongInfo', ['id', 'title', 'artist', 'rel_date'])


def remove_duplicates(s):
    return s[~s.index.duplicated()]


def interpolate(s, freq, startdate=None, enddate=None):
    if not startdate:
        startdate = s.index[0]
    if not enddate:
        enddate = s.index[-1]
    return s.asfreq('s').interpolate()[startdate:enddate].asfreq(freq)


__all__ = ['SongInfo', 'remove_duplicates', 'interpolate']
