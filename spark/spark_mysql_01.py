from typing import Dict, List, Optional, Tuple

from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class SparkClass:
    def __init__(self, appName: str, config: List[Tuple[str, str]] = None):
        self.app_name = appName
        if config:
            configs = SparkConf.setAll(config)
            self.spark = SparkSession.builder.appName(self.app_name).config(conf=configs).getOrCreate()
        else:
            self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()


    def init_sql(self, user, password, host, port: int = 3306, subprotocol: str = 'mysql'):
        self.user = user
        self.password = password
        self.host = host 
        self.port = port
        self.subprotocol = subprotocol

    
    def get_properties(self):

        driver = 'com.mysql.cj.jdbc.Driver'  # mysql 8
        return {
            'password': self.password,
            'user': self.user,
            'driver': driver
        }

    # Loading data from a JDBC source 
    def sql_to_df(self, db: str, table: str, properties: Optional[Dict[str, str]] = None):
        """
        properties = {
        	'characterEncoding': "utf8",
			'useSSL': "false"
            }
        """
        url = f'jdbc:{self.subprotocol}://{self.host}:{self.port}/{db}'
  
        p = self.get_properties()
        
        if properties:
            p.update(properties)
        DF = self.spark.read.jdbc(url=url, table=table, properties=p)
        return DF

    # Saving data to a JDBC source
    def df_to_sql(self, db: str, table: str, df: DataFrame, mode='ignore', columntype: str = None, properties: Optional[Dict[str, str]] = None):
        """
        columntype = "name CHAR(64), comments VARCHAR(1024)"
        """

        url = f'jdbc:{self.subprotocol}://{self.host}:{self.port}/{db}'
        p = self.get_properties()
        
        if properties:
            p.update(properties)

        if columntype:
            df.write \
                .option("createTableColumnTypes", columntype) \
                .jdbc(url, table, mode=mode, properties=p)

        else:
            df.write.jdbc(url, table, mode=mode, properties=p)




if __name__ == "__main__":
    user, password, host, port = 'root', 'ADSFabc@!#123456**22', '10.4.56.9', 13306
    properties = {
        	'characterEncoding': "utf8",
			'useSSL': "false"
            }

    spark_cls = SparkClass("test-to-mysql")
    spark_cls.init_sql(user, password, host, port)
    df = spark_cls.sql_to_df('water-resource', 'defaultDiversionData', properties)
    df.createOrReplaceTempView("test")
    res = spark_cls.spark.sql("select WaterCount from test")
    # WaterCount 为DF的Schema
    spark_cls.df_to_sql('dbtest', 'vvv', res, columntype="WaterCount FLOAT", properties=properties)
    
    spark_cls.spark.stop()
