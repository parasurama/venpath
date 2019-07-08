#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: get_counts
Description: Get counts of the raw gzipped files and processed data
Author: Prasanna Parasurama
"""

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# raw gzipped files

rdd = sc.textFile("/data/share/venpath/snowball/*/*/*/*.gz")
num_records_raw = rdd.count()

print(num_records_raw)

with open('raw_counts.txt', 'wr') as f:
    f.write(str(num_records_raw))








