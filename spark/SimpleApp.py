from pyspark.sql import SparkSession

logFile = "work-dir/test.txt"

if __name__ == '__main__':

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

    logData = spark.read.text(logFile).cache()   # 读取文件 并cache 缓存

    numAs = logData.filter(logData.value.contains('a')).count()
    numBs = logData.filter(logData.value.contains('b')).count()

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
    
    spark.stop()



'''
echo -e "this is a test\n\rhello world\n\raa bb" > test.txt
# 交互模式
/opt/spark/bin/pyspark \
--conf spark.kubernetes.container.image=apache/spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1  \
--conf spark.files=test.txt \   # 指定上传的测试文件

# cluster 模式
./bin/spark-submit \
--deploy-mode cluster \
--conf spark.kubernetes.container.image=apache/spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1  \
--conf spark.kubernetes.file.upload.path=/opt/spark/work-dir  \   # 将pi.py 文件上传到改目录下
work-dir/spark_df_01.py
# 通过kubectl logs -f XXX-driver  查看日志输出
'''

