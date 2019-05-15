#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: rdd_repartition
Description: process raw gzipped csv files, and repartition to manageable chunks
Author: Prasanna Parasurama
"""

from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
rdd = sc.textFile("/data/share/venpath/snowball/*/*/*/*.gz")

rdd = rdd.repartition(numPartitions=60000)

# save as splittable snappy files
rdd.saveAsTextFile("/data/share/venpath/rdd_partition",
                   compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")