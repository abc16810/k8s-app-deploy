# coding: utf-8

import os
import tempfile
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, size, split, to_timestamp, window

"""
基于Structured Streaming 读取TCP Socket读取数据，事件时间窗口统计词频，将结果打印到控制台
    每5秒钟统计最近10秒内的数据，设置水位Watermark时间为10秒
dog,2019-10-10 12:00:06
owl,2019-10-10 12:00:08	
dog,2019-10-10 12:00:14
cat,2019-10-10 12:00:09
cat,2019-10-10 12:00:15
dog,2019-10-10 12:00:08
owl,2019-10-10 12:00:13
owl,2019-10-10 12:00:21	
owl,2019-10-10 12:00:17

"""

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    lines  = spark.readStream \
        .format("socket") \
        .option("host", "10.4.56.230") \
        .option("port", 9999) \
        .load()

    # 过滤
    words = lines.filter(size(split('value', ',')) == 2)


    words = words.select(expr("(split(value, ','))[0]").alias('word'),  expr("(split(value, ','))[1]").alias('str_time')) \
        .select('word', to_timestamp('str_time').alias('time'))

    # 每5秒统计最近10秒内数据
    # 设置水位Watermark
    windowedCounts = words \
        .withWatermark("time", "10 seconds") \
        .groupBy(
            window('time', "10 seconds", "5 seconds"),
            'word') \
        .count()  


    query = windowedCounts.writeStream.outputMode("update").format("console").option('truncate', 'false').start()
            

    query.awaitTermination()
    query.stop()

"""
./bin/spark-submit \
--driver-class-path=/opt/spark/work-dir/mysql-connector-j.jar \
--jars /opt/spark/work-dir/mysql-connector-j.jar  \
--deploy-mode cluster --conf spark.kubernetes.container.image=apache/my-spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark --conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1  --conf spark.kubernetes.file.upload.path=/opt/spark/work-dir \
--conf spark.sql.shuffle.partitions=2 work-dir/xxx.py
"""
