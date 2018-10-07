#!/usr/bin/env python3
# coding: utf-8

import json
import logging
import os
import random

import arrow
import pandas as pd
import requests

from ._scrapers import (
    scrape_credits,
    scrape_albuminfo,
    scrape_requirements,
    scrape_songinfo,
    scrape_stats,
    scrape_top200,
    SONGURL,
    ALBUMURL
)
from ._utils import (
    interpolate,
)

logging.basicConfig(level=logging.DEBUG)


class Song(object):
    """This class represents a single song from a database.

    It's not meant to be instantiated directly, but instances of it are
    created on demand by the parent SongDB object.

    Attributes:
        id: The unique identifier of the song on Genie Music.
        title: The title of the song.
        artist: The artist of the song.
        is_tracking: Whether the song is currently being tracked or not.
            Please refer to the SongDB docstring for more information on
            the tracking system.
        credits: A dictionary containing the songwriting credits for the
            song as reported by Genie.
        minute: The minute at which the song will be fetched by the
            parent database.
    """
    def __init__(self, db, songid):
        self.id = songid
        self._info = db._songs[self.id]
        self.title = self._info.get('title')
        self.artist = self._info.get('artist')
        self.agency = self._info.get('agency')
        self.release_date = arrow.get(self._info.get('release_date')
        self._dbpath = os.path.join(db.path, '{}.pkl'.format(self.id))

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
    def minute(self):
        if 'minute' not in self._info:
            random.seed(self.id)
            self._info['minute'] = random.randrange(1, 60)
        return self._info['minute']

    def fetch(self):
        """Fetches the current total play count from Genie and stores it."""
        # scraping code
        try:
            page = requests.get(SONGURL, {'xgnm': self.id})
        except (requests.ConnectionError, requests.HTTPError):
            logging.error('Request to genie.co.kr for song ID %s failed',
                          self.id)
            return
        markup = page.text
        if not self.credits:
            self.credits = scrape_credits(markup)
        tstamp = arrow.get(page.headers.get('date'),
                           'ddd, DD MMM YYYY HH:mm:ss ZZZ')
        stats = scrape_stats(markup)

        # prepare the record to be stored
        record = pd.DataFrame(stats, [pd.to_datetime(tstamp.datetime)])
        # save new record in the database
        self._dbappend(record)
        logging.debug('Fetching completed: %s by %s',
                      self.title, self.artist)

    def _get_stats(self):
        data = self._db
        if data.empty:
            return data
        else:
            data = interpolate(data)
            data = data.floordiv(1)  # truncate the decimal part
            data = (-data.diff(-1)).head(-1)
            return data.to_period()

    def get_plays(self):
        """Returns a table of hourly streaming data.

        Returns:
            A Pandas Series object with a hourly PeriodIndex. The values
            represent the number of plays in the hour period. A record such as

            2018-09-18 11:00    3017

            means that the song has been played 3017 times in the time period
            from 11:00 to 11:59 of September 18, 2018. If not enough data have
            been fetched to return such a table an empty Series object will
            be returned.
        """
        return self._get_stats()['plays'].rename(self.title)

    def _dbappend(self, record):
        db = self._db.append(record)
        db = db.drop_duplicates()
        db.to_pickle(self._dbpath)

    @property
    def _db(self):
        return pd.read_pickle(self._dbpath)


class SongDB(object):
    """This class represents a single database.

    It provides methods to access the streaming data and keep the
    database up to date. Songs in the database, represented by Song
    objects, can be accessed either by iterating on the SongDB instance
    or by indexing, using their song ID.

    When creating a new database, it is advised to call the init_db()
    function found in this module, which will also return a SongDB
    instance to access the newly created database.

    The database is designed to fetch the total play count of the songs
    it keeps track of every hour, while adding new songs by looking at
    the hourly Genie Top 200. These operations, executed by the fetch()
    and update() methods respectively, need to be automated by the user
    of the package through a daemon, cron jobs or similar. A server
    module might be added to the package at a later date.

    To avoid overloading the Genie servers with requests, the database
    isn't designed to fetch streaming data for all the songs at the same
    time. Rather, a minute is assigned algorithmically to every song and
    the fetch() method will only retrieve data for the songs alloted to
    the minute at which the method is called. For example, the song
    뚜두뚜두 (DDU-DU DDU-DU) by BLACKPINK is scheduled to be fetched at
    the 17th minute of every hour, and will be only fetched if the
    fetch() method of the SongDB instance is called at that time.

    In order to prevent the average load to exceed one request per
    second, a limited number of songs can be tracked at any given time
    and when the database exceeds that size a number of songs stops
    being tracked further through a call to the prune() method.

    Attributes:
        path: the path to the directory where the database files are stored.
        quota: the maximum number of songs the database can track at any
            given moment. It's currently hardcoded to 3540, but it can be
            overridden at runtime and it will be configurable in a
            future release.
        tracking: the number of songs that are currently being tracked.
    """

    def __init__(self, path):
        """Returns a new SongDB object.

        Args:
            path: the path to the directory where the file structure of the
                database is located. Use init_db() to initialize a new database.

        Returns:
            A SongDB instance pointing to the database found in the path.

        Raises:
            FileNotFoundError: a file or directory required by the database has
                not been found.
        """
        self.path = path
        self.quota = 3540  # TODO make it configurable in the settings
        self._jsonpath = os.path.join(self.path, 'songs.json')
        self._blacklistpath = os.path.join(self.path, 'blacklist.json')
        self._songs = {}
        self._cache = {}
        self.load()

    def __getitem__(self, key):
        if key not in self._cache:
            self._cache[key] = Song(self, key)
        return self._cache[key]

    def __iter__(self):
        for songid in self._songs:
            yield self[songid]

    def __len__(self):
        return len(self._songs)

    def __contains__(self, item):
        return item in self._songs

    def is_tracking(self, songid):
        """Tells if the provided song ID is currently being tracked."""
        try:
            return self[songid].is_tracking
        except KeyError:
            return False

    @property
    def tracking(self):
        """Returns the number of songs currently being tracked."""
        return len([song for song in self if song.is_tracking])

    def prune(self, n):
        """Stops n currently tracking songs from being tracked.

        The songs are chosen based on their streaming performance.
        Specifically, the n songs with the least average plays/hour in the last
        10 days will stop being tracked. The are not removed from the database,
        and their tracking can be resumed if they are found again in the hourly
        Top 200 in a future call to SongDB.update().

        Args:
            n: the number of songs to be pruned.
        """
        # rank the song ids by streams in the last 10 days
        performance = {}
        for song in self:
            streams = song.get_plays().to_timestamp().last('10D').mean()
            performance[song.id] = streams
        for songid in sorted(performance, key=performance.get)[:n]:
            self[songid].is_tracking = False
        logging.debug('Disabled tracking of %d songs', min(n, performance))

    def add_from_songid(self, songid):
        """Fetches metadata for the song ID provided and adds it to the database."""
        songinfo = scrape_songinfo(songid)
        self.add_from_songinfo(songinfo)

    def add_from_songinfo(self, songinfo):
        """Adds to the database the song with the metadata provided.

        Args:
            songinfo: a dictionary with keys 'id', 'title', 'artist',
                'release_date' and 'agency'.
        """
        self._songs[songinfo['id']] = {'title': songinfo['title'],
                                       'artist': songinfo['artist'],
                                       'release_date':
                                           songinfo['release_date'].for_json(),
                                       'is_tracking': True,
                                       'credits': {},
                                       'agency': songinfo['agency']}

        dbpath = os.path.join(self.path, '{}.pkl'.format(songinfo['id']))
        pd.DataFrame(columns=['plays', 'listeners']).to_pickle(dbpath)
        logging.debug('Added to database (%s by %s)',
                      songinfo['title'], songinfo['artist'])

    def load(self):
        """Loads the song metadata and the blacklist in memory.

        This is called by the SongDB constructor, but can also be called later
        if one wants to revert the state of the SongDB object to what it was
        after the last call to SongDB.save().
        """
        with open(self._jsonpath, 'r') as f:
            self._songs = json.load(f)
        with open(self._blacklistpath, 'r') as f:
            self.blacklist = json.load(f)
        logging.debug('Song metadata DB and blacklist loaded')

    def save(self):  # TODO make JSON formatting configurable
        """Saves the current state of the song metadata and the blacklist.

        This is generally called after a call to SongDB.update() or fetch().
        """
        with open(self._jsonpath, 'w') as f:
            json.dump(self._songs, f, indent=4, ensure_ascii=False)
        with open(self._blacklistpath, 'w') as f:
            json.dump(self.blacklist, f, indent=0)
        logging.debug('Changes to the DB in memory saved on disk')

    def update(self):  # TODO change the order of stuff for better logging
        """Looks at the hourly Genie Top 200 and adds new songs to the database.

        In order to be added by this method, a song must satisfy the following
        criteria:

        - it must be tagged as '가요' in its genre field. This is the main tag
        for Korean-language songs, with the exception of OSTs.

        - it must be tagged as a title track in the album it's part of.

        Songs that don't meet these requirements, or have not charted, can be
        tracked by calling the add_from_songid() or add_from_songinfo methods.
        """
        tracking = self.tracking
        resume_tracking = []
        to_add = []

        with requests.Session() as session:
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
                # fetch album info and requirements
                try:
                    page = session.get(ALBUMURL,
                                       params={'axnm': song['album_id']})
                except (requests.ConnectionError, requests.HTTPError):
                    logging.error('Request to genie.co.kr for album ID %s failed. '
                                  'Song ID %s will not be added',
                                  song['album_id'], song['id'])
                    continue
                albuminfo = scrape_albuminfo(page.text)
                is_korean, is_title = scrape_requirements(page.text, song['id'])
                logging.debug('Info fetched for assessment (%s by %s)',
                              song['title'], song['artist'])
                # check requirements and add song
                if is_korean and is_title:
                    tracking += 1
                    songinfo = {'id': song['id'],
                                'title': song['title'],
                                'artist': song['artist'],
                                'release_date': albuminfo['release_date'],
                                'agency': albuminfo['agency']}
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

    def fetch(self, minute=arrow.utcnow().minute):
        """Calls Song.fetch() for the songs scheduled for the given minute.

        Args:
            minute: the minute for which the fetching must be performed. All
                songs that have the given minute in their minute attribute will
                be fetched. The argument is optional, and it defaults to the
                current minute as provided by the system clock.
        """
        logging.debug('Fetching started for minute %d', minute)
        # TODO change the order of actions so that I can log whether
        # anything was fetched this minute or not
        for song in self:
            if song.is_tracking and song.minute == minute:
                song.fetch()


def init_db(path):
    """Initializes a new database at the given path.

    Args:
        path: the path to the directory where the file structure of the
            new database will be created. If the directory doesn't
            exist, it will be created. It will also overwrite an
            existing database at the location.
    Returns:
        a SongDB instance pointing to the newly created database.
    """
    jsonpath = os.path.join(path, 'songs.json')
    blacklistpath = os.path.join(path, 'blacklist.json')
    if not os.path.isdir(path):
        os.makedirs(path)
    with open(jsonpath, 'w') as f:
        json.dump({}, f)
    with open(blacklistpath, 'w') as f:
        json.dump([], f)
    return SongDB(path)


__all__ = ['Song', 'SongDB', 'init_db']
