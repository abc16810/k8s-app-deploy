from pyspark.sql import SparkSession
from pyspark.sql.types import *

# 1. CSV 格式数据文本文件数据 -> 依据 CSV文件首行是否是列名称，决定读取数据方式不一样的

## CSV 格式数据：
#       每行数据各个字段使用逗号隔开
#       也可以指的是，每行数据各个字段使用 单一 分割符 隔开数据


# 首行不是列名，需要自定义Schema信息
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("readCSV")\
        .getOrCreate()
    # 首行是列名称, 分隔符为;
    # df = spark.read.options(delimiter=";", header=True).csv(path)
    df = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option('header', True) \
        .option("inferSchema", "true").load("work-dir/source/people.csv")   #  inferSchema 依据数值自动推断数据类型
        
    # 首行不是列名
    schema = StructType([
        StructField('name', StringType(), True), 
        StructField('age', IntegerType(), True), 
        StructField('job', StringType(), True)])
    df = spark.read.format("csv").schema(schema).option("delimiter", ";").load("work-dir/source/people.csv")
    
    df.printSchema()
    df.show(10, truncate = false)