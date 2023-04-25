from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, trim

# 进行词频统计，基于SQL分析

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()


    lines  = spark.readStream \
        .format("socket") \
        .option("host", "10.4.56.230") \
        .option("port", 9999) \
        .load()

    # 将DataFrame注册为临时视图
    lines.createOrReplaceTempView("word_counts")

    result = spark.sql("with tmp as (select explode(split(trim(value), ' ')) as word from word_counts) select word, count(1) as count from tmp group by word")

    query = result\
            .writeStream\
            .outputMode('update')\
            .format('console')\
            .option("truncate", "false") \
            .start()

    query.awaitTermination()
    query.stop()