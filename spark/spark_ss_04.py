# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, expr, split, udf
from pyspark.sql.types import BooleanType

## 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果打印到控制台
## 设置输出模式、查询名称、触发间隔及检查点位置

istrue = udf(lambda s: len(s) > 0, BooleanType())



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
    # 设置查询名称
    # 设置检查点目录
    query = words.writeStream.outputMode('update').format('console') \
        .queryName("query-wordcount") \
        .trigger(processingTime='0 seconds') \
        .option('numRows', '500')\
        .option('truncate', 'false')\
        .option("checkpointLocation", "/tmp/0001") \
        .start()
            
	## step5 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()

"""
./bin/spark-submit \
--deploy-mode cluster \
--conf spark.kubernetes.container.image=apache/spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1  \
--conf spark.kubernetes.file.upload.path=/opt/spark/work-dir  \  
work-dir/spark_ss_03.py
"""
