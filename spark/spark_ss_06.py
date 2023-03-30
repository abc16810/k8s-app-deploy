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

## foreach 测试 将数据写入到MySQL表中
istrue = udf(lambda s: len(s) > 0, BooleanType())

class ForeachWriter:
    def __init__(self):
        self.dbparams = dict(
            host="10.4.56.9",
            db='dbtest',
            user='root',
            passwd='ADSFabc@!#123456**22',
            port=13306,
            charset='utf8',
            connect_timeout=10,
        )
    def open(self, partition_id, epoch_id):
        self.conn = pymysql.connect(**self.dbparams)
        self.cursor = self.conn.cursor()
        self.res = []
        return True  # 返回true，表示成功
    def process(self, row):
        print('执行sql')
        # 针对DataFrame操作，每条数据类型就是Row
        if row:
            data = row.asDict()
            self.res.append((data.get('word'), data.get('count')))
    def close(self, error):
        print(error)
        self._do_insert_ro_update()
        self.cursor.close()
        self.conn.close()
    def _do_insert_ro_update(self):
        """插入数据库"""
        sql = """REPLACE INTO `test` (`word`, `count`) VALUES (%s, %s)"""
        if self.res:
            print('一共%s条数据' % len(self.res))
            self.cursor.executemany(sql, self.res)
            self.conn.commit()
        # if item:
        #     data = item.asDict()
        #     sql = """REPLACE INTO `test` (`word`, `count`) VALUES ('%(word)s', %(count)s)"""  % data
        #     self.cursor.execute(sql)


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
    query = words.writeStream.outputMode('update').foreach(ForeachWriter()).option("checkpointLocation", "/tmp/001").start()
            
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
--conf spark.kubernetes.file.upload.path=/opt/spark/work-dir \
--conf spark.sql.shuffle.partitions=2 \
work-dir/spark_ss_06.py
"""
