# coding: utf-8

import os
import tempfile
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, expr, split, udf
from pyspark.sql.types import BooleanType
from twisted.enterprise import adbapi

## 使用foreachBatch将词频统计结果输出到MySQL表中
istrue = udf(lambda s: len(s) > 0, BooleanType())


db_target_properties = {"user":"root", "password":"ADSFabc@!#123456**22", "driver": "com.mysql.cj.jdbc.Driver"}
db_target_url = f'jdbc:mysql://10.4.56.9:13306/dbtest'

def foreach_batch_function(df, epoch_id):
    print("开始写入mysql epoch_id %s" % epoch_id)
    df.write.jdbc(url=db_target_url, table="test", mode="overwrite", properties=db_target_properties)


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("file-test")\
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    lines  = spark.readStream \
        .format("socket") \
        .option("host", "10.4.56.230") \
        .option("port", 9999) \
        .load()

    ## 进行词频统计
    words = lines.select(
        # explosion将数组中的每个项转换为单独的行
        explode(
            split(lines.value, ' ')
        ).alias('word')
    ).filter(istrue('word')).groupBy('word').count()


    # 设置输出模式， 当数据更新时再进行输出
    query = words.writeStream.outputMode('complete').foreachBatch(foreach_batch_function) \
    .option("checkpointLocation", "/var/data/test-001").start()
            
	## 启动流式应用后，等待终止
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
