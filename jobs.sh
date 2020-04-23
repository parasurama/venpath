#!/usr/bin/env bash

spark-submit --num-executors 200 --executor-memory 2g --conf spark.yarn.maxAppAttempts=1 --conf spark.executor.memoryOverhead=1g etl/repartition_transform.py
spark-submit --num-executors 200 --executor-memory 2g --conf spark.yarn.maxAppAttempts=1 --conf spark.executor.memoryOverhead=1g jobs/get_raw_counts.py
spark-submit --num-executors 200 --executor-memory 4g --driver-memory 4g --conf spark.yarn.maxAppAttempts=1 jobs/get_processed_counts.py
spark-submit --num-executors 200 --executor-memory 4g --driver-memory 16g --conf spark.yarn.maxAppAttempts=1 --conf spark.executor.memoryOverhead=2g etl/process_covid_files.py