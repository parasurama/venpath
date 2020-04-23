#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
Author: Prasanna Parasurama
"""


from pyspark.sql.types import IntegerType, DoubleType, BooleanType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()

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

df = spark.read.csv("/user/pp1994/venpath/covid/raw_data/venpath_covid/*.gz")

# rename columns
for k, v in col_names_map.items():
    df = df.withColumnRenamed(k, v)

# change types
# df = df.withColumn("venpath_id", df["venpath_id"].cast(LongType()))
df = df.withColumn("lat", df["lat"].cast(DoubleType()))
df = df.withColumn("lon", df["lon"].cast(DoubleType()))
df = df.withColumn("timestamp", to_timestamp(df["timestamp"], "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("horizontal_accuracy", df["horizontal_accuracy"].cast(IntegerType()))
df = df.withColumn("vertical_accuracy", df["vertical_accuracy"].cast(IntegerType()))
df = df.withColumn("foreground", df["foreground"].cast(BooleanType()))

# pings
df\
    .select("ad_id", "lat", "lon", "timestamp", "horizontal_accuracy", "foreground") \
    .sort("ad_id")\
    .write\
    .parquet("/user/pp1994/venpath/covid/pings")

# static
df\
    .select("ad_id", "app_id", "id_type", "country_type", "device_make", "device_model", "device_os", "device_os_version")\
    .dropDuplicates()\
    .sort('ad_id')\
    .write\
    .parquet("/user/pp1994/venpath/covid/static")
