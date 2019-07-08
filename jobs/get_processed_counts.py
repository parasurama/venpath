#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: get_processed_counts
Description: 
Author: Prasanna Parasurama
"""

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


df = spark.read.parquet("/scratch/pp1994/venpath/pings/*/*/*/*.parquet")

count = df.count()

print(count)

with open('raw_counts.txt', 'wr') as f:
    f.write(str(count))

