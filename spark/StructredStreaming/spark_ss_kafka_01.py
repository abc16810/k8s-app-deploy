# coding: utf-8

import os
import tempfile
import uuid

import pymysql
from pymysql.cursors import DictCursor
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, expr, split, udf
from pyspark.sql.types import BooleanType
from twisted.enterprise import adbapi

# "kafka.security.protocol" : "SASL_PLAINTEXT",   # 指定与borkers代理通信的协议 默认SASL_SSL




if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("file-test")\
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    lines  = spark.readStream.format('kafka') \
        .option("kafka.bootstrap.servers", "kafka-1.kafka-headless.kafka.svc.cluster.local:9092") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option('subscribe', 'wordsTopic') \
        .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='user1' password='password1';") \
        .load().selectExpr("CAST(value AS STRING)")

    ## 进行词频统计
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )


    wordCounts = words.groupBy('word').count()
    # 设置输出模式， 当数据更新时再进行输出
    query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .queryName("wordcount-kafka") \
        .start()
            
	## 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()

"""
# 需要将依赖的jar包打包到镜像中
/opt/spark/bin/pyspark  \
--conf spark.kubernetes.container.image=apache/my-spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1 --conf spark.sql.shuffle.partitions=2



/opt/spark/bin/pyspark  --jars /opt/spark/work-dir/spark-streaming-kafka-0-10_2.12-3.3.2.jar,/opt/spark/work-dir/spark-sql-kafka-0-10_2.12-3.3.2.jar  --conf spark.kubernetes.container.image=apache/my-spark-py:v3.3.2 --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark --conf spark.kubernetes.namespace=spark --conf spark.executor.instances=1  --conf spark.sql.shuffle.partitions=2
"""
