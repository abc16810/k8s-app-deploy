# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, expr, split

## 文件数据源（File Source）：将目录中写入的文件作为数据流读取，支持的文件格式为：text、csv、json、orc、parquet
## 使用Structured Streaming从目录中读取文件数据：统计年龄小于20岁的人群的爱好排行榜

if __name__ == "__main__":
    # setup1 构建SparkSession对象，配置相关信息
    spark = SparkSession\
        .builder\
        .appName("file-test")\
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # setup2 从文件数据源加载数据，监听目录下的文件
    # 行分隔符默认处理所有“\r”、“\r\n”和“\n”  文本数据源只生成一个名为“value”的数据列。
    inputStreamDF:DataFrame = spark.readStream.format("text").load("/opt/spark/work-dir/source")   # dir equal .text(path)

    # setup3 通过expr 将value列分成多列 或者通过withColumn+ udf函数
    # a.withColumn("name", split(a.value, ' ').getItem(0)).withColumn("age", split(a.value, ' ').getItem(1).cast('int')).drop(col('value'))
    newDF: DataFrame =  inputStreamDF.select( 
        expr("(split(value, ' '))[0]").alias('name'),  
        expr("(split(value, ' '))[1]").alias('age').cast('int'),
        expr("(split(value, ' '))[2]").alias('score').cast('int')
        )

    # setup4 统计年龄小于30岁的人群的分数排行榜
    resultStreamDF: DataFrame = newDF.filter(col('age') < 30 ).groupBy('score').count().sort('score')

    # step4 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    # update 模式输出    
    query = resultStreamDF\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option('numRows', '10')\
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
work-dir/spark_ss_02.py
"""
## make data
"""
def make_data(num=1000):
    for _ in range(num):
        name = fake.name()
        age = random.randint(0,50)
        socre = random.randint(50,100)
        yield name, age, socre



data = make_data(num=100)
with open("aa.txt", "a+", encoding='utf-8') as f:
    for x in data:
        f.write(' '.join([ str(k) for k in x ]))  
        f.write("\t\n") 
"""