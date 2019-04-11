#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: sample_test
Description: Testing sample
Author: Prasanna Parasurama
"""

from pyspark.sql.types import IntegerType, DoubleType, BooleanType, LongType
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

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


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    # 2 sample files
    df = spark.read.csv("/data/share/venpath/rdd_partition/*.snappy")

    # rename columns
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
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("date", dayofmonth("timestamp"))
    df = df.withColumn("lat_int", floor(df["lat"] / 10)*10)
    df = df.withColumn("lon_int", floor(df["lon"] / 10)*10)

    df\
        .select("ad_id", "lat", "lon", "timestamp", "horizontal_accuracy", "foreground",
                "year", "month", "date", "lat_int", "lon_int")\
        .repartition("lat_int", "lon_int", "year", "month", "date")\
        .write\
        .partitionBy("lat_int", "lon_int", "year", "month", "date")\
        .parquet("/data/share/venpath/lat_lon_time_partitioned", mode="overwrite")
