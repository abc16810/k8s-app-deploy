import pymysql
from pool import MysqlPool, UsingMysql, mysqlpool
from pymysql.cursors import DictCursor
from pyspark.sql import SparkConf, SparkContext, SparkSession

## Could not serialize object: TypeError: cannot pickle 'socket' object

# def save_to_mysql(iterator):
#     mysqlpool.get_conn()
#     cursor = mysqlpool.cursor
#     sql = "INSERT into aaa(`WaterCount`) values(%s)"
#     data = [(record,) for record in iterator]
#     cursor.executemany(sql, data)
#     mysqlpool.conn.commit()
#     mysqlpool.close()

# def save_to_mysql(iterator):
#     with  UsingMysql(log_time=True) as mysqlpool:
#         
#         
#         


def save_to_mysql(iterator):
    connection = mysqlpool.getConnection()
    
    
    connection.cursor.executemany(sql, data)
    connection.commit()
    connection.cursor.close()
    mysqlpool.returnConnection(connection)

class Save:
    def __init__(self, tableName):
        self.table_name = tableName
    @staticmethod
    def get_properties():
        return  dict(
                host='10.4.56.9',
                port=13306,
                db='dbtest',
                user='root',
                passwd='ADSFabc@!#123456**22',
                charset='utf8mb4',
                connect_timeout=10,
            )
    def get_conn(self):
        return pymysql.connect(**self.get_properties())
    def __call__(self, iterator):
        conn = self.get_conn()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        data = [(record,) for record in iterator]
        sql = "INSERT into {}(`WaterCount`) values(%s)".format(self.table_name)
        print("开始写入mysql 表名称为 %s  length %s" % (self.table_name, len(data)))
        print(sql)
        cursor.executemany(sql, data)
        conn.commit()
        cursor.close()
        conn.close()
        

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()
    # conf = SparkConf().setAppName('appName')
    # sc = SparkContext(conf=conf)

    # 读取文件数据，sc.textFile方法，将数据封装到RDD中
    sc = spark.sparkContext
    rdd = sc.textFile('work-dir/source/aa.txt')  # 本地文件（所有节点）
    # 通过算子flapMap、map和reduceByKey 处理
    counts = rdd.flatMap(lambda x: x.strip().split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y: x+y)
    
    # 将结果输出或者保存文件
    # counts.saveAsTextFile('work-dir/success')
    # counts.foreach(lambda x: print(x))
    # save_to_mysql = Save('aaa')
    # counts.coalesce(1).foreachPartition(save_to_mysql)  # coalesce(1)  合并分区  foreachPartition   每个分区数据插入数据库时，创建一个连接Connection
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    sc.stop()
