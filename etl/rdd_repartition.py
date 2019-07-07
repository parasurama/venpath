#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: rdd_repartition
Description: process raw gzipped csv files, and repartition to manageable chunks
Author: Prasanna Parasurama
"""

from pyspark.context import SparkContext

fpaths = ["/data/share/venpath/snowball/2016/06",
          "/data/share/venpath/snowball/2016/07",
          "/data/share/venpath/snowball/2016/08",
          "/data/share/venpath/snowball/2016/09",
          "/data/share/venpath/snowball/2016/10",
          "/data/share/venpath/snowball/2016/11",
          "/data/share/venpath/snowball/2016/12",
          "/data/share/venpath/snowball/2017/01",
          "/data/share/venpath/snowball/2017/02",
          "/data/share/venpath/snowball/2017/03",
          "/data/share/venpath/snowball/2017/04",
          "/data/share/venpath/snowball/2017/05",
          "/data/share/venpath/snowball/2017/06",
          "/data/share/venpath/snowball/2017/07",
          "/data/share/venpath/snowball/2017/08",
          "/data/share/venpath/snowball/2017/09",
          "/data/share/venpath/snowball/2017/10"]


sc = SparkContext.getOrCreate()
rdd = sc.textFile("/data/share/venpath/snowball/*/*/*/*.gz")

rdd = rdd.repartition(numPartitions=100000)

# save as splittable snappy files
rdd.saveAsTextFile("/data/share/venpath/rdd_partition",
                   compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")