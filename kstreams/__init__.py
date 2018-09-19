#!/usr/bin/env python3
# coding: utf-8

"""
Create and maintain a database of k-pop digital streaming statistics

This package provides an object oriented interface to a database of
streaming data, fetched from Genie Music through the methods of the
classes in this package.

Usage of the package revolves around two classes:

- the SongDB class represents a single database, and exposes the
  interface of an iterable collection of Song objects.

- the Song class represent a single entry in a database.

Please refer to the docstrings of the classes to learn more about
their usage.
"""

from .classes import Song, SongDB, init_db

name = 'kstreams'
__all__ = ['Song', 'SongDB', 'init_db']
