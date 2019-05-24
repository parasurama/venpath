#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: repartition_transform
Description: 
Author: Prasanna Parasurama
"""

from pyspark.sql.types import IntegerType, DoubleType, BooleanType, LongType
from pyspark.sql.functions import to_timestamp, dayofmonth
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

import os

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


def get_file_id(fpath):
    year = fpath.split('/')[-2]
    month = fpath.split('/')[-1].lstrip('0')
    file_id = year+"_"+month
    return file_id


def read_repartition_write_rdd(fpath):
    print("repartitioning rdd ", fpath)

    rdd = sc.textFile(fpath+"/*/*.gz")

    rdd = rdd.repartition(numPartitions=2500)

    # save as splittable snappy files
    file_id = get_file_id(fpath)
    rdd.saveAsTextFile("/scratch/pp1994/venpath/temp_rdd/{}".format(file_id),
                       compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")


def delete_temp_rdd():
    os.system("hadoop fs -rm -r -skipTrash /scratch/pp1994/venpath/temp_rdd")


def transform_and_write_df(fpath):
    print("transforming df ", fpath)

    # 2 sample files
    file_id = get_file_id(fpath)
    year, month = file_id.split('_')

    df = spark.read.csv("/scratch/pp1994/venpath/temp_rdd/{}/*.snappy".format(file_id))

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
        .withColumn("date", dayofmonth("timestamp"))\
        .select("ad_id", "lat", "lon", "timestamp", "horizontal_accuracy", "foreground", "date")\
        .repartition("date", numPartitions=5)\
        .sortWithinPartitions('lat', 'lon')\
        .write\
        .partitionBy("date")\
        .parquet("/scratch/pp1994/venpath/pings/year={}/month={}".format(year, month))

    # static
    df\
        .select("ad_id", "app_id", "id_type", "country_type", "device_make", "device_model", "device_os", "device_os_version")\
        .dropDuplicates()\
        .sort('ad_id')\
        .write\
        .parquet("/scratch/pp1994/venpath/static/{}".format(file_id))


def process_data(fpath):
    file_id = get_file_id(fpath)
    existing_rdd_id = os.popen("hadoop fs -ls /scratch/pp1994/venpath/temp_rdd").read().split()[-1].split('/')[-1]

    if file_id != existing_rdd_id:
        read_repartition_write_rdd(fpath)

    transform_and_write_df(fpath)
    delete_temp_rdd()


if __name__ == "__main__":

    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    for f in fpaths[1:3]:
        process_data(f)
