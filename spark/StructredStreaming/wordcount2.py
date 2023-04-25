# coding: utf-8

import os
import tempfile
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, size, split

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # includeTimestamp 增加读取时间
    read_lines  = spark.readStream \
        .format("socket") \
        .option("host", "10.4.56.230") \
        .option('includeTimestamp', 'true')\
        .option("port", 9999) \
        .load()

    words = read_lines.select(
        explode(split('value', ' ')).alias('word'),
        'timestamp'
    )

    query = words.writeStream.outputMode("update").format("console").option('truncate', 'false').start()
            
	## step5 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()

"""
./bin/spark-submit \
--driver-class-path=/opt/spark/work-dir/mysql-connector-j.jar \
--jars /opt/spark/work-dir/mysql-connector-j.jar  \
--deploy-mode cluster --conf spark.kubernetes.container.image=apache/my-spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark --conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1  --conf spark.kubernetes.file.upload.path=/opt/spark/work-dir \
--conf spark.sql.shuffle.partitions=2 work-dir/spark_ss_07.py
"""
