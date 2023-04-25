# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, expr, split

## 内置数据源之Rate Source 使用
## 以每秒指定的行数生成数据，每个输出行包含2个字段：timestamp和value。
# 其中timestamp是一个Timestamp含有信息分配的时间类型，并且value是Long（包含消息的计数从0开始作为第一行）类型

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("file-test")\
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # 从Rate数据源实时消费数据。rowsPerSecond 默认1 每秒生成多少行数据， numPartitions 默认为 parallelism
    inputStreamDF:DataFrame = spark.readStream \
    		.format("rate") \
    		.option("rowsPerSecond", "50") \
            .option("numPartitions", "2") \
    		.load()

    # 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    query = inputStreamDF\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .option('numRows', '500')\
        .option('truncate', 'false')\
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
