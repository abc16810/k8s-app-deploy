
import datetime
import json
import random
import time

from pyspark.sql.types import (ArrayType, DecimalType, IntegerType, StringType,
                               StructField, StructType)

allStatus = [0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


def get_random_ip():
    range_ip = (
            (607649792,608174079),  # 36.56.0.0-36.63.255.255
            (1038614528,1039007743), #61.232.0.0-61.237.255.255
            (1783627776,1784676351), #106.80.0.0-106.95.255.255
            (2035023872,2035154943), #121.76.0.0-121.77.255.255
            (2078801920,2079064063), #123.232.0.0-123.235.255.255
            (-1950089216,-1948778497), #139.196.0.0-139.215.255.255
            (-1425539072,-1425014785), #171.8.0.0-171.15.255.255
            (-1236271104,-1235419137), #182.80.0.0-182.92.255.255
            (-770113536,-768606209), #210.25.0.0-210.47.255.255
            (-569376768,-564133889)  #222.16.0.0-222.95.255.255
        )
    index = random.randint(0, 9)
    ipNumber = range_ip[index][0] + random.randint(1, range_ip[index][1] - range_ip[index][0])
    num2ip = '%s.%s.%s.%s' % (ipNumber >> 24 & 0xff, ipNumber >> 16 & 0xff, ipNumber >> 8 & 0xff, ipNumber  & 0xff)
    return num2ip

def mockData():
    n = 0 
    res = []
    # 生产100条数据
    while n < 100:
        currentTime = int(time.time() * 1000)
        orderId = "%d%06d" % (currentTime, n)
        userId = "%d%08d" % (1 + random.randint(0, 5), random.randint(1, 1000))
        orderTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        orderMoney = "%d.%02d" % (5 + random.randint(1,500), random.randint(1,100))
        orderStatus = random.choice(allStatus)
        getRandomIp = get_random_ip()
        n += 1
        # 订单记录数据
        orderId, userId, orderTime, getRandomIp, orderMoney, orderStatus
        item = json.dumps(dict(
                orderId=orderId,
                userId= userId,
                orderTime= orderTime,
                getRandomIp= getRandomIp,
                orderMoney = orderMoney,
                orderStatus = orderStatus
            ))
        res.append((None, item))
    return res


def mock_data(sleep=10, topic="test-d"):
    data = mockData()
    rdd = spark.sparkContext.parallelize(data)
    schema = StructType([  StructField('key', StringType(), True), StructField('value', StringType(), True)])
    df = rdd.toDF(schema)
    while True:
        df.write.format('kafka') \
        .option("kafka.bootstrap.servers", "kafka-1.kafka-headless.kafka.svc.cluster.local:9092") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option('topic', topic).save()
        # .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='admin123';") \
        time.sleep(sleep)

"""
/opt/spark/bin/pyspark  \
--conf spark.kubernetes.container.image=apache/my-spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  --conf spark.executor.instances=1 \
--conf spark.sql.shuffle.partitions=3  --conf spark.executor.cores=3 \
--driver-java-options "-Djava.security.auth.login.config=/opt/spark/work-dir/jaas.conf" \
--conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/opt/spark/work-dir/jaas.conf
"""


# pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, get_json_object, udf


@udf(returnType=ArrayType(IntegerType()))
def ip_to_region(ip):
    """
    通过IP地址返回所在城市及省份
    """
    province = random.randint(1,9)
    city = random.randint(province*10, province*10+9)
    return (province, city)

def save_to_mysql(df, info):
    k = Save(info)
    df.writeStream.queryName(info).outputMode('complete'). \
        foreachBatch(k).option("checkpointLocation", "/tmp/instance-01").start()


class Save:
    def __init__(self, tableName):
        self.table_name = tableName
        self.url = f'jdbc:mysql://10.4.56.9:13306/dbtest'
    @staticmethod
    def get_properties():
        return  {"user":"root", "password":"ADSFabc@!#123456**22", "driver": "com.mysql.cj.jdbc.Driver"}
    def __call__(self, df, epoch_id):
        print("开始写入mysql 表名称为 %s epoch_id %s" % (self.table_name, epoch_id))
        df.coalesce(1) \
            .write.jdbc(
                url=self.url, 
                table="test_%s" % self.table_name, 
                mode="overwrite", 
                properties=self.get_properties())


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("ETL")\
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

    items  = spark.readStream.format('kafka') \
        .option("kafka.bootstrap.servers", "kafka-1.kafka-headless.kafka.svc.cluster.local:9092") \
        .option('subscribe', 'test-d') \
        .option("maxOffsetsPerTrigger", "10000") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .load().selectExpr("CAST(value AS STRING)")

    frame = items.select(
        get_json_object('value', '$.getRandomIp').alias('ip'),
        get_json_object('value', '$.orderMoney').cast(DecimalType(10,2)).alias('money'),
        get_json_object('value', '$.orderStatus').alias('status'),
        ) \
        .filter("status == 0") \
        .withColumn("region", ip_to_region(col("ip"))) \
        .select(col('region')[0].alias('province'), col('region')[1].alias('city'), 'money')
    
    
    frame.createOrReplaceTempView("tmp_view")
     
    f1 = spark.sql("SELECT '国家' as type, SUM(money) as totalMoney   FROM tmp_view")
    # frame.agg(sum('money').alias('totalMoney'))

    f2 = spark.sql("SELECT province as type, SUM(money) as totalMoney   FROM tmp_view GROUP BY province")
    # frame.groupBy('province').agg(sum('money').alias('totalMoney'))

    f3 = spark.sql("SELECT city as type, SUM(money) as totalMoney   FROM (SELECT * FROM tmp_view WHERE city in (20, 30))t GROUP BY t.city")
    # frame.filter(col('city').isin([20,30])).groupBy('city').agg(sum('money').alias('totalMoney'))


    save_to_mysql(f1, 'total')
    save_to_mysql(f2, 'totalprovince')
    save_to_mysql(f3, 'totalcity')
            
	## 启动流式应用后，等待终止
    spark.streams.awaitAnyTermination()