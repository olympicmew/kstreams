#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 17:41:51 2018

@author: olympicmew
"""

import random
import os
import json
import collections.abc
import pandas as pd
from datetime import timedelta
import arrow
from urllib.request import urlopen
import logging
from .utils import (
    SongInfo,
    remove_duplicates,
    interpolate
)
from .scrapers import (
    scrape_top200,
    scrape_streams,
    scrape_credits,
    scrape_releasedate,
    scrape_requirements,
    scrape_songinfo
)

logging.basicConfig(filename='kstreams-test.log', level=logging.DEBUG)

SONGURL = 'http://www.genie.co.kr/detail/songInfo?xgnm={}'
ALBUMURL = 'http://www.genie.co.kr/detail/albumInfo?axnm={}'


class Song(object):
    def __init__(self, db, songid):
        self.id = songid
        self._info = db._songs[self.id]
        self._dbpath = os.path.join(db.path, self.id)

    @property
    def title(self):
        return self._info['title']

    @property
    def artist(self):
        return self._info['artist']

    @property
    def is_tracking(self):
        return self._info['is_tracking']

    @is_tracking.setter
    def is_tracking(self, value):
        self._info['is_tracking'] = value

    @property
    def credits(self):
        return self._info['credits']

    @credits.setter
    def credits(self, value):
        self._info['credits'] = value

    @property
    def fetch_min(self):
        if 'fetch_min' not in self._info:
            random.seed(self.id)
            self._info['fetch_min'] = random.randrange(1, 60)
        return self._info['fetch_min']

    def fetch(self, fetch_credits=False):

        # scraping code
        url = SONGURL.format(self.id)
        # TODO logging
        with urlopen(url) as page:
            markup = page.read().decode()
            if fetch_credits:
                self.credits = scrape_credits(markup)
            tstamp = arrow.get(page.getheader('Date'),
                               'ddd, DD MMM YYYY HH:mm:ss ZZZ')
            streams = scrape_streams(markup)

        # prepare the record to be stored
        record = pd.Series(streams, [pd.to_datetime(tstamp.datetime)],
                           name=self.title)
        # save new record in the database
        self._dbappend(record)
        logging.debug('Fetching completed: %s by %s',
                      self.title, self.artist)

    @property
    def plays(self):
        # retains the interface with the hourly gain
        # TODO a function that gives out the raw data
        data = pd.read_pickle(self._dbpath)
        if data.empty:
            return data
        else:
            data = remove_duplicates(data)
            startdate = arrow.get(data.index[0]).floor('hour').shift(hours=1)
            data = interpolate(data, 'h', startdate=startdate.datetime)
            data = data.astype(int)
            return data.diff().shift(-1, freq='h').to_period()

    def _dbappend(self, record):
        self.plays.append(record).to_pickle(self._dbpath)


class SongDB(collections.abc.Mapping):

    def __init__(self, path):
        self.path = path
        self._jsonpath = os.path.join(self.path, 'songs.json')
        self._blacklistpath = os.path.join(self.path, 'blacklist.json')
        self._cache = {}
        self.load()

    def __getitem__(self, key):
        if key not in self._cache:
            self._cache[key] = Song(self, key)
        return self._cache[key]

    def __iter__(self):
        return iter(self._songs)

    def __len__(self):
        return len(self._songs)

    def is_tracking(self, id):
        return self[id].is_tracking

    @property
    def quota(self):
        return 3540  # TODO make it configurable in the settings

    @property
    def tracking(self):
        return len([song for song in self.values() if song.is_tracking])

    def prune(self, n):
        # rank the song ids by streams in the last 10 days and stop tracking
        # the last n elements
        perf = {}
        for song in self.values():
            streams = song.plays.to_timestamp().last('10D').sum()
            perf[song.id] = streams
        for songid in sorted(perf, key=perf.get)[:n]:
            self[songid].is_tracking = False
        logging.debug('Disabled tracking of %d songs', n)

    def add_from_songid(self, songid):
        songinfo = scrape_songinfo(songid)
        self.add_from_songinfo(songinfo)

    def add_from_songinfo(self, songinfo):
        self._songs[songinfo.id] = {'title': songinfo.title,
                                    'artist': songinfo.artist,
                                    'is_tracking': True,
                                    'credits': {}}

        dbpath = os.path.join(self.path, songinfo.id)
        pd.Series(name=songinfo.title).to_pickle(dbpath)
        logging.debug('Added to database (%s by %s)',
                      songinfo.title, songinfo.artist)

    def load(self):
        with open(self._jsonpath, 'r') as f:
            self._songs = json.load(f)
        with open(self._blacklistpath, 'r') as f:
            self.blacklist = json.load(f)
        logging.debug('Song metadata DB and blacklist loaded')

    def save(self):  # TODO make JSON formatting configurable
        with open(self._jsonpath, 'w') as f:
            json.dump(self._songs, f)
        with open(self._blacklistpath, 'w') as f:
            json.dump(self.blacklist, f)
        logging.debug('Changes to the DB in memory saved on disk')

    def update(self):  # TODO change the order of stuff for better logging
        tracking = self.tracking
        resume_tracking = []
        to_add = []

        for song in scrape_top200():

            # skip blacklisted songs
            if song['id'] in self.blacklist:
                logging.debug('Skipped: blacklisted (%s by %s)',
                              song['title'], song['artist'])
                continue

            # catch songs already in the db
            if song['id'] in self:
                if self.is_tracking(song['id']):
                    logging.debug('Skipped: already tracking (%s by %s)',
                                  song['title'], song['artist'])
                    continue
                else:
                    tracking += 1
                    resume_tracking.append(song['id'])
                    logging.debug('Tracking will be resumed (%s by %s)',
                                  song['title'], song['artist'])
                    continue

            url = ALBUMURL.format(song['album_id'])
            with urlopen(url) as page:
                markup = page.read().decode()
                rel_date = scrape_releasedate(markup)
                is_korean, is_title = scrape_requirements(markup, song['id'])
            logging.debug('Info fetched for assessment (%s by %s)',
                          song['title'], song['artist'])
            days_since_rel = (arrow.utcnow() - rel_date) / timedelta(days=1)

            if is_korean and is_title and days_since_rel < 100999:
                tracking += 1
                songinfo = SongInfo(song['id'],
                                    song['title'],
                                    song['artist'],
                                    rel_date.for_json())
                to_add.append(songinfo)
            else:
                self.blacklist.append(song['id'])
                logging.debug('Blacklisted (%s by %s)',
                              song['title'], song['artist'])

        # check if quota is exceeded with new songs and make space
        if tracking > self.quota:
            self.prune(tracking - self.quota)

        for songid in resume_tracking:
            self[songid].is_tracking = True
        if len(resume_tracking):
            logging.debug('%d songs: tracking resumed', len(resume_tracking))
        for songinfo in to_add:
            self.add_from_songinfo(songinfo)
        logging.debug('%d songs: added to the database', len(to_add))
        # self.save()

    def fetch(self, current_min):
        logging.debug('Fetching started for minute %d', current_min)
        # TODO change the order of actions so that I can log whether
        # anything was fetched this minute or not
        for song in self.values():
            if song.is_tracking and song.fetch_min == current_min:
                if not song.credits:
                    song.fetch(fetch_credits=True)
                else:
                    song.fetch()


def init_db(path):
    jsonpath = os.path.join(path, 'songs.json')
    blacklistpath = os.path.join(path, 'blacklist.json')
    if not os.path.isdir(path):
        os.mkdir(path)
    with open(jsonpath, 'w') as f:
        json.dump({}, f)
    with open(blacklistpath, 'w') as f:
        json.dump([], f)