#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
An ugly script that will do the job of fetching data until I'll come
around writing something better
"""

import logging
logging.basicConfig(filename='kstreams.log', level=logging.DEBUG)

if __name__ == '__main__':
    import kstreams
    from sys import argv

    if argv[1] == 'init':
        kstreams.init_db('db')
    elif argv[1] == 'update':
        db = kstreams.SongDB('db')
        db.update()
        db.save()
    elif argv[1] == 'fetch':
        db = kstreams.SongDB('db')
        db.fetch()
        db.save()
