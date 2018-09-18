#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 20:59:41 2018

@author: olympicmew
"""

from urllib.request import urlopen
from bs4 import BeautifulSoup
from .utils import SongInfo
import arrow
import re
import logging
logging.basicConfig(filename='kstreams-test.log', level=logging.DEBUG)


def scrape_top200():
    url = 'http://www.genie.co.kr/chart/top200?ditc=D&rtm=Y&pg={}'
    songs = []
    for n in range(1, 5):
        page = urlopen(url.format(n))
        markup = page.read().decode()
        soup = BeautifulSoup(markup, 'lxml')
        entries = soup.find('tbody').find_all('tr')
        for entry in entries:
            songid = entry.get('songid')
            title = entry.find(class_='title').get_text().strip()
            artist = entry.find(class_='artist').get_text().strip()
            albumid = entry.find(class_='albumtitle').get('onclick')
            regex = re.compile(r"fnViewAlbumLayer\('(.+)'\)")
            albumid = regex.search(albumid).group(1)
            song = {'id': songid,
                    'title': title,
                    'artist': artist,
                    'album_id': albumid}
            songs.append(song)
        logging.debug('Page %d parsed', n)
    logging.debug('Scraping of top 200 completed')
    return songs


def scrape_requirements(markup, songid):
    soup = BeautifulSoup(markup, "lxml")
    genre = soup.find(alt='장르/스타일').parent.find_next_sibling(class_='value')
    is_korean = '가요' in genre.get_text()
    is_title = bool(soup.find(songid=songid).find(class_='icon-title'))
    return is_korean, is_title


def scrape_releasedate(markup):
    # TODO default assumed release time configurable in settings
    # default is 9am UTC unless such an assumption would imply time travel,
    # fallback is top of previous hour
    soup = BeautifulSoup(markup, "lxml")
    rel_date = soup.find(alt='발매일').parent.find_next_sibling(class_='value')
    rel_date = arrow.get(rel_date.get_text().strip())
    rel_date = rel_date.replace(hour=9)
    now = arrow.utcnow()
    if rel_date > now:
        rel_date = rel_date.replace(hour=now.hour).shift(hours=-1)
    return rel_date


def scrape_streams(markup):
    soup = BeautifulSoup(markup, "lxml")
    streams = soup.find(alt='전체 재생수').parent.find_previous_sibling('p')
    streams = streams.get_text().strip().replace(',', '')
    return int(streams)


def scrape_credits(markup):
    soup = BeautifulSoup(markup, "lxml")

    try:
        lyr = soup.find(alt='작사가').parent.find_next_sibling(class_='value')
        lyr = [s.strip() for s in lyr.get_text().split(',')]
    except AttributeError:
        lyr = []

    try:
        comp = soup.find(alt='작곡가').parent.find_next_sibling(class_='value')
        comp = [s.strip() for s in comp.get_text().split(',')]
    except AttributeError:
        comp = []

    try:
        arr = soup.find(alt='편곡자').parent.find_next_sibling(class_='value')
        arr = [s.strip() for s in arr.get_text().split(',')]
    except AttributeError:
        arr = []

    return {'lyrics': lyr, 'composition': comp, 'arrangement': arr}


def scrape_songinfo(songid):
    from .kstreams import SONGURL, ALBUMURL
    page = urlopen(SONGURL.format(songid))
    markup = page.read().decode()
    soup = BeautifulSoup(markup, 'lxml')
    title = soup.find(class_='name').get_text().strip()

    def find_artist(tag):
        return tag.has_attr('onclick') and 'artistInfo' in tag.get('onclick')
    artist = soup.find(find_artist)
    artist = artist.get_text().strip()

    def find_albumid(tag):
        return tag.has_attr('onclick') and 'albumInfo' in tag.get('onclick')
    albumid = soup.find(find_albumid).get('onclick')
    albumid = re.compile(r"'([0-9]+)'").search(albumid).group(1)
    albumpage = urlopen(ALBUMURL.format(albumid))
    rel_date = scrape_releasedate(albumpage.read().decode())

    return SongInfo(songid, title, artist, rel_date.for_json())


__all__ = ['scrape_top200',
           'scrape_requirements',
           'scrape_releasedate',
           'scrape_streams',
           'scrape_credits',
           'scrape_songinfo']
