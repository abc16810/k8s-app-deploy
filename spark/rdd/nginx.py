import time

from pyspark.sql import SparkSession


def parser(line):
    """
    分割数据
    """
    item = line.split('"')
    item = [ x.strip() for x in item if x ]
    v0 = item[0].split(" ") # 180.163.30.85 - - [18/Oct/2022:19:40:51 +0800] 
    remote_ip = v0[0]
    request_time = v0[3].replace('[', '')
    v1 = item[1].split(" ") # 'GET /path HTTP/1.1'
    request_method = v1[0]
    request_path = v1[1]
    request_version = v1[2]
    v2 = item[2].split(" ")  # '200 1234'
    status = v2[0]
    response_size = v2[1]
    agent = item[-1]
    return (remote_ip, request_time, request_method, request_path, request_version, status, response_size, agent)


def getTime_local_day(time_local: str):
    try:
        struct_time = time.strptime(time_local, '%d/%b/%Y:%H:%M:%S')
        return time.strftime('%Y-%m-%d', struct_time)
    except:
        return time_local





if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    sc = spark.sparkContext
    rdd = sc.textFile('work-dir/source/access.log')  # 本地文件（所有节点）
    # 
    new_rdd = rdd.filter(lambda x: len(x) > 100).mapPartitions(lambda x: map(parser, x))
    

    # 统计前10的访问IP
    pv_lines = new_rdd.map(lambda line: (line[0], 1))\
		.reduceByKey(lambda v1, v2: v1+v2)\
		.sortBy(lambda x: x[1], ascending=False).take(10)
    

    # 按照时间统计
    pv_time_lines = new_rdd.map(lambda x: (getTime_local_day(x[1]), 1)).reduceByKey(lambda v1, v2: v1+v2) \
        .sortBy(lambda x: x[1], ascending=False).take(10)


    # 接口URL
    pv_url_lines = new_rdd.map(lambda x: (x[3], 1)).reduceByKey(lambda v1, v2: v1+v2) \
        .sortBy(lambda x: x[1], ascending=False).take(10)   

    # 方法
    pv_method_lines = new_rdd.map(lambda x: (x[2], 1)).reduceByKey(lambda v1, v2: v1+v2).sortBy(lambda x: x[1], ascending=False).take(10)

    # 状态码
    pv_status_lines = new_rdd.map(lambda x: (x[5], 1)).reduceByKey(lambda v1, v2: v1+v2).sortBy(lambda x: x[1], ascending=False).collect()

    # 流量
    seqOp = (lambda x, y: x + y)
    combOp = (lambda x, y: x + y)
    pv_sum_lines = new_rdd.map(lambda x: int(x[6])).aggregate(0, seqOp, combOp)