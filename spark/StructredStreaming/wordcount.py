# coding: utf-8

import os
import tempfile
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, explode, explode_outer, expr, size,
                                   split, to_timestamp, window)

"""
基于Structured Streaming 模块读取TCP Socket读取数据，进行事件时间窗口统计词频WordCount，将结果打印到控制台
    每5秒钟统计最近10秒内的数据（词频：WordCount)

EventTime即事件真正生成的时间：
    例如一个用户在10：06点击 了一个按钮，记录在系统中为10：06
    这条数据发送到Kafka，又到了Spark Streaming中处理，已经是10：08，这个处理的时间就是process Time。

测试数据：
	2019-10-12 09:00:02,cat dog
	2019-10-12 09:00:03,dog dog
	2019-10-12 09:00:07,owl cat
	2019-10-12 09:00:11,dog
	2019-10-12 09:00:13,owl
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

	# 将每行数据进行分割单词: 2019-10-12 09:00:02,cat dog
    #                                 time             key
	# 使用expr及split函数以后 -> 2019-10-12 09:00:02 [cat, dog]
    # 
    # explode_outer为给定数组或映射中的每个元素返回一个新行 
    #                                  time          key
    # 通过explode_outer函数后 -> |2019-10-12 09:00:02|cat|  
    #                            |2019-10-12 09:00:02|dog|
    words = words.select(expr("(split(value, ','))[0]").alias('name'),  expr("split((split(value, ','))[1], ' ')").alias('key')) \
        .select(to_timestamp('name').alias('time'), explode_outer('key').alias('word'))

    # 每5秒统计最近10秒内数据
    # 1. 先按照窗口分组、2. 再对窗口中按照单词分组、 3. 最后使用聚合函数聚合
    windowedCounts = words.groupBy(
        window('time', "10 seconds", "5 seconds"),
        'word'
    ).count().orderBy('window')   # 按照窗口字段降序排序


    query = windowedCounts.writeStream.outputMode("complete").trigger(processingTime='5 seconds').format("console").option('truncate', 'false').start()
            

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
