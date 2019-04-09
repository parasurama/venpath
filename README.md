# Venpath on CUSP

This 


## Accessing Data

We have 2 ways to access
1. Pyspark (interactive, but restrictions on resources)
2. spark-submit (batch, less restriction on resources)

### Pyspark (Interactive)

- Pyspark is the spark python API
- CUSP limits pyspark usage to 2 cores/user. So, at most you can gain 2x parallelism.   
```
$pyspark

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.0
      /_/

Using Python version 3.7.2 (default, Dec 29 2018 06:19:36)
SparkSession available as 'spark'.

>>> # loads data from 2016/6/1
>>> df = spark.read.parquet("/data/share/venpath/sample_df_transform/year=2016/month=6/date=1")

>>> # You can also use wildcards. loads data from all of 2016
>>> df = spark.read.parquet("/data/share/venpath/sample_df_transform/year=2016/month=*/date=*")
```

### spark-submit 

- spark-submit is used to run batch applications with python
- CUSP limits spark-submit usage to 86 cores/user. 

For example, to run the get_count.py script from shell:
```
$ spark-submit --num-executors 86 sample_scripts/get_count.py
```

There are other command line options you can adjust. See [here](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-submit.html).

To use python3.7:

```
$scl enable miniconda3 bash
$export PYSPARK_DRIVER_PYTHON=/opt/cdp/miniconda3/bin/python
$export PYSPARK_PYTHON=/opt/cdp/miniconda3/bin/python
```

## Monitoring Jobs

Once you 

### HUE 
- https://data.cusp.nyu.edu/hue/accounts/login
- HUE (Hadoop User Interface) lets you monitor running jobs at a high level

### SPARK UI

- On Spark UI, you can dive deeper into a particular job in more detail (see resources being used, which specific task is failing etc)

To access Spark UI
1. Log into Linux RDP on CUSP (https://serv.cusp.nyu.edu/dash_beta/#/)
2. Start mozilla and change the following settings in about-config:
![](assets/mozilla-settings.png)
3. Go to the Tracking URL (found in HUE --> job --> properties)

## Resources

- Pyspark Cheatsheet: https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_Cheat_Sheet_Python.pdf
- Intro to Spark Dataframes: https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html 
- Spark-sql documentation: https://spark.apache.org/docs/latest/sql-programming-guide.html

 

