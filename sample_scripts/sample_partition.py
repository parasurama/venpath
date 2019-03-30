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


def rename_columns(df):
    for k,v in col_names_map.items():
        df = df.withColumnRenamed(k, v)
    return df


def cast_as_type(df):
    df = df.withColumn("venpath_id", df["venpath_id"].cast(LongType()))
    df = df.withColumn("lat", df["lat"].cast(DoubleType()))
    df = df.withColumn("lon", df["lon"].cast(DoubleType()))
    df = df.withColumn("timestamp", to_timestamp(df["timestamp"], "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("horizontal_accuracy", df["horizontal_accuracy"].cast(IntegerType()))
    df = df.withColumn("vertical_accuracy", df["vertical_accuracy"].cast(IntegerType()))
    df = df.withColumn("foreground", df["foreground"].cast(BooleanType()))
    # partition by columsn
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("date", dayofmonth("timestamp"))
    return df


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    # 2 sample files
    dfs = spark.read.csv(["/data/share/venpath/snowball/2016/06/01/*.gz",
                          "/data/share/venpath/snowball/2016/07/01/*.gz"])

    dfs = rename_columns(dfs)
    dfs = cast_as_type(dfs)

    dfs\
        .repartition(5)\
        .write\
        .partitionBy("year", "month", "date")\
        .parquet("/data/share/venpath/sample_partition2")
