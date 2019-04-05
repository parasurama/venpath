#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: sample_partition_parallel
Description: 
Author: Prasanna Parasurama
"""

from itertools import product
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, LongType
from pyspark.sql.functions import to_timestamp, dayofmonth
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import re


col_names_map = {"_c0": "venpath_id",
                 "_c1": "app_id",
                 "_c2": "ad_id",
                 "_c3": "id_type",
                 "_c4": "country_type",
                 "_c5": "device_make",
                 "_c6": "device_model",
                 "_c7": "device_os",
                 "_c8": "device_os_version",
                 "_c9": "lat",
                 "_c10": "lon",
                 "_c11": "timestamp",
                 "_c12": "ip",
                 "_c13": "horizontal_accuracy",
                 "_c14": "vertical_accuracy",
                 "_c15": "foreground"}


def get_possible_paths():
    years = [2016, 2017]
    months = range(1, 13)
    dates = range(1, 32)
    date_combos = product(years, months, dates)
    base_path = "/data/share/venpath/snowball/{year}/{month}/{date}/*.gz"
    paths = []
    for d in date_combos:

        paths.append(base_path.format(year=d[0], month=d[1], date=d[2]))
    return paths


def read_transform_write(fpath):
    spark = SparkSession(SparkContext.getOrCreate())

    df = spark.read.csv(fpath)

    for k, v in col_names_map.items():
        df = df.withColumnRenamed(k, v)

    # change types
    df = df.withColumn("venpath_id", df["venpath_id"].cast(LongType()))
    df = df.withColumn("lat", df["lat"].cast(DoubleType()))
    df = df.withColumn("lon", df["lon"].cast(DoubleType()))
    df = df.withColumn("timestamp", to_timestamp(df["timestamp"], "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("horizontal_accuracy", df["horizontal_accuracy"].cast(IntegerType()))
    df = df.withColumn("vertical_accuracy", df["vertical_accuracy"].cast(IntegerType()))
    df = df.withColumn("foreground", df["foreground"].cast(BooleanType()))
    # partition by columns
    df = df.withColumn("date", dayofmonth("timestamp"))

    # write back as parquet

    year, month = tuple(fpath.split("/")[5:7])

    df\
        .repartition(5)\
        .write\
        .partitionBy("date")\
        .parquet("/data/share/venpath/sample_partition_parallel/{year}/{month}".format(year=year, month=month))

    return True


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    with open('fpaths.txt') as f:
        fpaths = f.read().split('\n')

    # sample
    fpaths = [x for x in fpaths if re.search(r"/2016/(06|07)/01", x)]

    paths = sc.parallelize(fpaths)
    paths.map(lambda x: read_transform_write(x))