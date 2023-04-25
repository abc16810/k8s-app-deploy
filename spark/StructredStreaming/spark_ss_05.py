# coding: utf-8

import os
import tempfile
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, expr, split, udf
from pyspark.sql.types import BooleanType

## foreach 测试
istrue = udf(lambda s: len(s) > 0, BooleanType())


def _write_event(dir, event):
    import uuid
    with open(os.path.join(dir, str(uuid.uuid4())), "w") as f:
        f.write("%s\n" % str(event))
class ForeachWriter:
    def open(self, partition_id, epoch_id):
        print("partition is %s" % str(partition_id) )
        print("epoch_id is %s" % str(epoch_id) )
        return True  # 返回true，表示成功
    def process(self, row):
        # Write row to connection. This method is NOT optional in Python.
        print('111')
        print(row)
    def close(self, error):
        # Close the connection. This method in optional in Python.
        print(error)



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
    query = words.writeStream.outputMode('update').foreach(ForeachWriter()).start()
            
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
--conf spark.kubernetes.file.upload.path=/opt/spark/work-dir  \  
work-dir/spark_ss_03.py
"""
