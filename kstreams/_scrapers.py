#!/usr/bin/env python3
# coding: utf-8

import logging
import re

import arrow
import requests
from bs4 import BeautifulSoup

from ._utils import SongInfo

TOP200URL = 'http://www.genie.co.kr/chart/top200'
SONGURL = 'http://www.genie.co.kr/detail/songInfo'
ALBUMURL = 'http://www.genie.co.kr/detail/albumInfo'


def scrape_top200():
    songs = []
    with requests.Session() as session:
        for n in range(1, 5):
            params = {'ditc': 'D', 'rtm': 'Y', 'pg': n}
            page = session.get(TOP200URL, params=params)
            soup = BeautifulSoup(page.text, 'lxml')
            entries = soup.find('tbody').find_all('tr')
            for entry in entries:
                songid = entry.get('songid')
                entry.find(class_='
                title = entry.find(class_='title')
                # remove age rating info from the title tag
                for span in title.find_all('span'):
                    span.decompose()
                title = title.get_text(strip=True)
                artist = entry.find(class_='artist').get_text(strip=True)
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
    rel_date = arrow.get(rel_date.get_text(strip=True))
    rel_date = rel_date.replace(hour=9)
    now = arrow.utcnow()
    if rel_date > now:
        rel_date = rel_date.replace(hour=now.hour).shift(hours=-1)
    return rel_date


def scrape_stats(markup):
    soup = BeautifulSoup(markup, "lxml")
    plays = soup.find(alt='전체 재생수').parent.find_previous_sibling('p')
    plays = plays.get_text(strip=True).replace(',', '')
    listeners = soup.find(alt='전체 청취자수').parent.find_previous_sibling('p')
    listeners = listeners.get_text(strip=True).replace(','. '')
    return {'plays': int(plays), 'listeners': int(listeners)}


def scrape_credits(markup):
    soup = BeautifulSoup(markup, "lxml")

    try:
        lyr = soup.find(alt='작사가').parent.find_next_sibling(class_='value')
        lyr = [s for s in lyr.get_text(strip=True).split(',')]
    except AttributeError:
        lyr = []

    try:
        comp = soup.find(alt='작곡가').parent.find_next_sibling(class_='value')
        comp = [s for s in comp.get_text(strip=True).split(',')]
    except AttributeError:
        comp = []

    try:
        arr = soup.find(alt='편곡자').parent.find_next_sibling(class_='value')
        arr = [s for s in arr.get_text(strip=True).split(',')]
    except AttributeError:
        arr = []

    return {'lyrics': lyr, 'composition': comp, 'arrangement': arr}


def scrape_songinfo(songid):
    with requests.Session() as session:
        page = session.get(SONGURL, params={'xgnm': songid})
        soup = BeautifulSoup(page.text, 'lxml')
        title = soup.find(class_='name')
        # remove age rating info from the title tag
        for span in title.find_all('span'):
            span.decompose()
        title = title.get_text(strip=True)

        def find_artist(tag):
            return (tag.has_attr('onclick') and
                    'artistInfo' in tag.get('onclick'))
        artist = soup.find(find_artist)
        artist = artist.get_text(strip=True)

        def find_albumid(tag):
            return (tag.has_attr('onclick') and
                    'albumInfo' in tag.get('onclick'))
        albumid = soup.find(find_albumid).get('onclick')
        albumid = re.compile(r"'([0-9]+)'").search(albumid).group(1)
        albumpage = session.get(ALBUMURL, params={'axnm': albumid})
        rel_date = scrape_releasedate(albumpage.text)

    return SongInfo(songid, title, artist, rel_date.for_json())


__all__ = []
