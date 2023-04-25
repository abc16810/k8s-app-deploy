# coding: utf-8

import os
import tempfile
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (col, explode, expr, size, split, struct,
                                   to_json, udf)
from pyspark.sql.types import BooleanType

"""
模拟产生运营商基站数据,实时发送到Kafka 中,使用StructuredStreaming消费,
经过ETL（获取通话状态为success数据）后,写入Kafka中,便于其他实时应用消费处理分析。
"""
# python-kafka3 目前最高支持kafka到2.6版本 
# from kafka3 import KafkaProducer

# producer = KafkaProducer(bootstrap_servers=['10.4.56.230:9096'], acks='1', 
#                             key_serializer=str.encode,
#                             value_serializer=str.encode,
#                             security_protocol="SASL_PLAINTEXT", 
#                             sasl_mechanism="SCRAM-SHA-256", 
#                             sasl_plain_username="admin", 
#                             sasl_plain_password="admin123")




# 模拟产生基站日志数据，实时发送Kafka Topic中，数据字段信息：
## 基站标识符ID, 主叫号码, 被叫号码, 通话状态, 通话时间，通话时长

import random
import time

from pyspark.sql.types import StringType, StructField, StructType

all_status = ["fail", "busy", "barring", "success", "success", "success",
            "success", "success", "success", "error", "success", "success"]

def mock():
    n = 0 
    res = []
    while n < 101:
        station = "station_%d"  % random.randint(1,10) 
        out = "1860000%04d" % (random.randint(1,10000))
        input = "1890000%04d" % (random.randint(1,10000))
        status = random.choice(all_status)
        
        now = time.time()*1000
        duration = (random.randint(1,10) + 1) * 1000 if status == 'success'  else 0
        data = '%s,%s,%s,%s,%s,%s' % (station, out, input, status, now, duration)
        res.append((None, data))
        n += 1
    return res


def mock_data(sleep=10, topic="test-a"):
    data = mock()
    rdd = spark.sparkContext.parallelize(data)
    schema = StructType([  StructField('key', StringType(), True), StructField('value', StringType(), True)])
    df = rdd.toDF(schema)
    while True:
        df.write.format('kafka') \
        .option("kafka.bootstrap.servers", "kafka-1.kafka-headless.kafka.svc.cluster.local:9092") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option('topic', topic) \
        .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='user1' password='password1';") \
        .save()
        time.sleep(sleep)


# mock_data()


# 实时增量ETL
# 编写应用实时从Kafka的【test-a】消费数据，经过处理分析后，存储至Kafka的【test-b】，其中需要设置检查点目录，保证应用一次且仅一次的语义
 
if __name__ == "__main__":

    len_fun = udf(lambda s: len(s) == 6, BooleanType())

    spark = SparkSession\
        .builder\
        .appName("ETL")\
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

    items  = spark.readStream.format('kafka') \
        .option("kafka.bootstrap.servers", "kafka-1.kafka-headless.kafka.svc.cluster.local:9092") \
        .option('subscribe', 'test-a') \
        .option("maxOffsetsPerTrigger", "10000") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .load().selectExpr("CAST(value AS STRING)")

    ## 只获取通话状态为success日志数据
    # etl_df = lines.filter(col('value').contains("success")).filter(len_fun(split('value', ',')))
    # size 返回数组的长度
    etl_df = items.filter(col('value').contains("success")).filter(size(split('value', ',')) == 6)
    # etl_df.rdd.map(lambda x: x[0].split(',')).toDF(["stationId", "callOut", "callIn", "callStatus", "callTime", "duration"]). \
    #   select(to_json(struct('*')).alias('value')) 

    # 最终将ETL的数据存储到Kafka Topic中
    query = etl_df\
        .writeStream\
        .queryName("state-etl") \
        .outputMode('append')\
        .option("checkpointLocation", "/tmp/10001") \
        .format('kafka')\
        .option("kafka.bootstrap.servers", "kafka-0.kafka-headless.kafka.svc.cluster.local:9092") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option('topic', 'test-b') \
        .start()
            
	## 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()


"""
./bin/spark-submit \
--deploy-mode cluster \
--conf spark.kubernetes.container.image=apache/my-spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1  \
--conf spark.executor.cores=3 \
--conf spark.kubernetes.file.upload.path=/opt/spark/work-dir \
--driver-java-options "-Djava.security.auth.login.config=/opt/spark/work-dir/jaas.conf" \
--conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/opt/spark/work-dir/jaas.conf \
work-dir/spark_ss_kafka_02.py
"""

"""
# cat jaas.conf
KafkaClient {
 org.apache.kafka.common.security.scram.ScramLoginModule required
 username="admin"
 password="admin123";
};
"""


