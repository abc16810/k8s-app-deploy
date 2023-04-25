# coding: utf-8

import os
import tempfile
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, split, udf


def mean_func(key, pdf):
    return pdf.DataFrame([key + (pdf.v.mean(),)])


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # 创建DataFrame表示从连接到[主机:端口]的输入行流,并包含时间列
    lines  = spark.readStream \
        .format("socket") \
        .option("host", "10.4.56.230") \
        .option("includeTimestamp", "true") \
        .option("port", 9999) \
        .load()

    # 将行分割为单词，保留时间戳，每个单词成为一个sessionId
    events = lines.select(
        explode(split(lines.value, " ")).alias("sessionId"),
        lines.timestamp,
    )

    sessions= events.groupby('id').applyInPandas(mean_func, schema="id string, v long")

    # step4 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    # update 模式输出
    
    query = sessions.writeStream.outputMode("update").format("console").option('truncate', 'false').start()
            
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
