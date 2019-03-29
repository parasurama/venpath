#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: get_count
Description: Get count of the entire dataset
Author: Prasanna Parasurama
"""

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.csv("/data/share/venpath/snowball/2016/06/*/*.gz")
count = df.count()
print(count)

