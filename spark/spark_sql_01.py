from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("AAA").getOrCreate()
    #.config("spark.some.config.option", "some-value") \
    sc = spark.sparkContext
    lines = sc.textFile("/user/hive/warehouse/cldasv2.db/nc/aa.txt")
    parts = lines.map(lambda line: line.split(","))
    people = parts.map(lambda p: (p[0], p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12]))
    schemaString = "a b c d e f g h i g k i l"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    schemaPeople = spark.createDataFrame(people, schema)
    schemaPeople.createOrReplaceTempView("people")
    results = spark.sql("SELECT a FROM people")
    results.show()