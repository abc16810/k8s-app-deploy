# 运行官方案例，从侦听TCP套接字的数据服务器接收到的文本数据
# 消费数据，进行词频统计，打印控制台

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

## 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果打印到控制台
# 程序入口SparkSession，加载流式数据：spark.readStream
# 数据封装Dataset/DataFrame中，分析数据时，建议使用DSL编程，调用API，很少使用SQL方式
# 启动流式应用，设置Output结果相关信息、start方法启动应用
## 
if __name__ == "__main__":
    # setup1 构建SparkSession对象，配置相关信息
    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # setup2 从TCP Socket加载数据，读取数据列名称为value，类型是String
    lines  = spark.readStream \
        .format("socket") \
        .option("host", "10.4.56.230") \
        .option("port", 9999) \
        .load()

    # setup3 进行词频统计，基于SQL分析
    words = lines.select(
        # explosion将数组中的每个项转换为单独的行
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

    wordCounts = words.groupBy('word').count()

    # step4 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    # update 模式输出
    
    query = wordCounts\
        .writeStream\
        .outputMode('update')\
        .format('console')\
        .option('numRows', '10')\
        .option('truncate', 'false')\
        .start()
            
	## step5 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()


## nc -lk 9999
"""
./bin/spark-submit \
--deploy-mode cluster \
--conf spark.kubernetes.container.image=apache/spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1  \
--conf spark.kubernetes.file.upload.path=/opt/spark/work-dir  \  
work-dir/spark_ss_01.py
"""