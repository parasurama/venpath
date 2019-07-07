#!/usr/bin/env bash

spark-submit --num-executors 200 --executor-memory 2g --conf spark.yarn.maxAppAttempts=1 --conf spark.executor.memoryOverhead=1g etl/repartition_transform.py
spark-submit --num-executors 200 --executor-memory 2g --conf spark.yarn.maxAppAttempts=1 --conf spark.executor.memoryOverhead=1g jobs/get_counts.py