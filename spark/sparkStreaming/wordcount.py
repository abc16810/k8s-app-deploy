# -*- coding: utf-8 -*

# spark streaming 测试
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
	# 创建一个本地StreamingContext，有两个工作线程，批处理间隔为1秒
	sc = SparkContext("local[2]", appName="NetworkWordCount")  # yarn local[2]
	# sc.addPyFile("../common")
	ssc = StreamingContext(sc, 1)

	# 使用这个上下文，我们可以创建一个DStream，它表示来自TCP源的流数据，指定为主机名(例如localhost)和端口(例如9999)
	# 创建一个将连接到 hostname:port，如localhost:9999
	lines = ssc.socketTextStream("192.168.10.20", 9999)
	# DStream lines表示将从数据服务器接收到的数据流。这个DStream中的每个记录都是一行文本
	# 把每一行分成单词
	words = lines.flatMap(lambda line: line.split(" "))
	# flatMap是一个一对多的DStream操作，它通过从源DStream中的每个记录生成多个新记录来创建一个新的DStream
	# 每一行将被分割成多个单词，单词流表示为单词DStream。接下来，我们要计算这些单词。
	# 计算每批中的每个单词
	pairs = words.map(lambda word: (word, 1))
	wordCounts = pairs.reduceByKey(lambda x, y: x+y)

	# 将这个DStream中生成的每个RDD的前十个元素打印到控制台
	wordCounts.pprint()

	# Spark Streaming只设置了它在启动时将要执行的计算，没有真正的处理开始。要在设置完所有转换后开始处理，我们最后调用
	ssc.start()  # #启动计算
	ssc.awaitTermination()  # 等待计算结束

