#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: filter
Description: 
Author: Prasanna Parasurama
"""

import pandas as pd
import time


def test_lat_filter(df):
    st = time.time()
    print(df.filter(df["lat"].between(41, 42)).count())
    print(time.time() - st)


def test_lon_filter(df):
    st = time.time()
    print(df.filter(df["lon"].between(-105, -104)).count())
    print(time.time() - st)


def test_time_filter(df):
    st = time.time()
    df.filter(df["timestamp"].between(pd.to_datetime('2016-06-03'), pd.to_datetime('2016-06-05'))).count()
    print(time.time() - st)


def test_all_filter(df):
    st = time.time()
    print(df.filter(df["lat"].between(41, 42) & df["lon"].between(-105, -104) & df["timestamp"].between(pd.to_datetime('2016-06-03'), pd.to_datetime('2016-06-05'))).count())
    print(time.time() - st)
