#!/home/ubuntu/venv-kstreams/bin/python3
# -*- coding: utf-8 -*-

"""
An ugly script that will do the job of fetching data until I'll come
around writing something better
"""

import logging
logging.basicConfig(filename='kstreams.log', level=logging.DEBUG)

if __name__ == '__main__':
    import argparse
    import kstreams

    parser = argparse.ArgumentParser()
    parser.add_argument('mode',
                        choices=['init', 'update', 'update-newest', 'fetch'],
                        required=True)
    args = parser.parse_args()

    if args.mode == 'init':
        kstreams.init_db('db')
    elif args.mode == 'update':
        db = kstreams.SongDB('db')
        db.update()
        db.save()
    elif args.mode == 'update-newest':
        db = kstreams.SongDB('db')
        db.update(fetch_newest=True)
        db.save()
    elif args.mode == 'fetch':
        db = kstreams.SongDB('db')
        db.fetch()
        db.save()
