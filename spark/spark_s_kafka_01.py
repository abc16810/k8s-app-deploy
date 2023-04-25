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
# "kafka.sasl.mechanism"   默认SCRAM-SHA-512

# pyspark
"""
# 需要将依赖的jar包打包到镜像中
/opt/spark/bin/pyspark  \
--conf spark.kubernetes.container.image=apache/my-spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1 --conf spark.sql.shuffle.partitions=2
"""
# kafka
"""
kafka-acls.sh --bootstrap-server localhost:9092 \
--command-config /tmp/client.properties \
--add --allow-principal User:'user1' --operation READ \
--topic topicName --group='spark-kafka-relation-' \
--resource-pattern-type prefixed 
"""


# 从kafka读取数据
df_get  = spark.read.format('kafka') \
    .option("kafka.bootstrap.servers", "kafka-1.kafka-headless.kafka.svc.cluster.local:9092") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option('subscribe', 'test-a') \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='admin123';") \
    .load()

# 写入到kafka
from pyspark.sql.types import StringType, StructField, StructType

data = [('Finance', 10), ('Marketing', 20), ('Sales', 30), ('IT', 40)]
rdd = spark.sparkContext.parallelize(data)
cc = StructType([  StructField('key', StringType(), True), StructField('value', StringType(), True)])
df = rdd.toDF(cc)
df.write.format('kafka') \
.option("kafka.bootstrap.servers", "kafka-1.kafka-headless.kafka.svc.cluster.local:9092") \
.option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
.option("kafka.security.protocol", "SASL_PLAINTEXT") \
.option('topic', 'test-a') \
.option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='user1' password='password1';") \
.save()
