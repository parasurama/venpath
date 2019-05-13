#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: repartition_rdd
Description: 
Author: Prasanna Parasurama
"""
from pyspark.context import SparkContext


sc = SparkContext.getOrCreate()
rdd = sc.textFile("/data/share/venpath/snowball/*/*/*/*.gz")

rdd = rdd.repartition(rdd.getNumPartitions()*25)

rdd.saveAsTextFile("/data/share/venpath/rdd_partition",
                   compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")
