#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
An ugly script that will do the job of fetching data until I'll come
around writing something better
"""

if __name__ == '__main__':
    import logging
    import argparse
    import kstreams
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument('mode',
                        choices=['init', 'update', 'update-newest', 'fetch'])
    parser.add_argument('path', nargs='?', default=None)
    parser.add_argument('-v', '--verbose', action='count', default=0)
    args = parser.parse_args()

    if args.path:
        path = args.path
    else:
        path = 'db'

    if args.verbose >= 2:
        loglevel = logging.DEBUG
        handlers = [logging.FileHandler('kstreams.log'),
                    logging.StreamHandler()]
    elif args.verbose == 1:
        loglevel = logging.INFO
        handlers = [logging.FileHandler('kstreams.log'),
                    logging.StreamHandler()]
    else:
        loglevel = logging.INFO
        handlers = [logging.FileHandler('kstreams.log')]
    logging.basicConfig(level=loglevel, handlers=handlers)

    if args.mode == 'init':
        kstreams.init_db(path)
    elif args.mode == 'update':
        db = kstreams.SongDB(path)
        db.update()
        db.save()
    elif args.mode == 'update-newest':
        db = kstreams.SongDB(path)
        db.update(fetch_newest=True)
        db.save()
    elif args.mode == 'fetch':
        db = kstreams.SongDB(path)
        db.fetch()
        db.save()
    sys.exit()
