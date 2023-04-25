# DBUtils == 3.0.2

from queue import Queue
from timeit import default_timer

import pymysql
from dbutils.pooled_db import PooledDB
from pymysql.cursors import DictCursor


class MysqlPool(object):
    """
        MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现
        获取连接对象：conn = Mysql.getConn()
        释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None
    def __init__(self, mincached=2, maxcached=5, maxshared=3, maxconnections=6, blocking=True,
                    maxusage=0, setsession=None, reset=True,
                    host='10.4.56.9', port=13306, db='dbtest',
                    user='root', passwd='ADSFabc@!#123456**22', charset='utf8mb4'):
        """
        :param mincached:连接池中空闲连接的初始数量
        :param maxcached:连接池中空闲连接的最大数量
        :param maxshared:共享连接的最大数量
        :param maxconnections:创建连接池的最大数量
        :param blocking:超过最大连接数量时候的表现，为True等待连接数量下降，为false直接报错处理
        :param maxusage:单个连接的最大重复使用次数
        """
        if self.__pool is None:
            self.__class__.__pool = PooledDB(pymysql, 
                                            mincached, maxcached,
                                            maxshared, maxconnections, blocking,
                                            maxusage, setsession, reset,
                                            host=host,
                                            port=port, 
                                            user=user,
                                            passwd=passwd,
                                            db=db, 
                                            charset=charset,
                                            connect_timeout=3)
                                            #cursorclass=DictCursor
            self._conn = None
            self._cursor = None
            self.__get_conn()
    def __get_conn(self):
        self.conn = self.__pool.connection()
        self.cursor = self.conn.cursor()
    def get_conn(self):
        return self.__pool.connection()
    def close(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            print(e)
    
    @staticmethod
    def __dict_datetime_obj_to_str(result_dict):
        """把字典里面的datatime对象转成字符串，使json转换不出错"""
        if result_dict:
            result_replace = {k: v.__str__() for k, v in result_dict.items() if isinstance(v, datetime.datetime)}
            result_dict.update(result_replace)
        return result_dict

    def __execute(self, sql, param=()):
        count = self.cursor.execute(sql, param)
        return count

    def dictfetchall(self):
        "Return all rows from a cursor as a dict"
        columns = [col[0] for col in self.cursor.description]

        return [
            dict(zip(columns, row))
            for row in self.cursor.fetchall()
        ]

    def select_one(self, sql, param=()):
        """查询单个结果"""
        count = self.__execute(sql, param)
        result = self.cursor.fetchone()
        """:type result:dict"""
        return count, result


    def select_many(self, sql, param=()):
        """
        查询多个结果
        :param sql: qsl语句
        :param param: sql参数
        :return: 结果数量和查询结果集
        """
        count = self.__execute(sql, param)
        result = self.cursor.fetchall()

        return count, result

    def begin(self):
        """开启事务"""
        self.conn.autocommit(0)

    def end(self, option='commit'):
        """结束事务"""
        if option == 'commit':
            self.conn.autocommit()
        else:
            self.conn.rollback()


mysqlpool = MysqlPool()



class UsingMysql(object):
    def __init__(self, commit=True, log_time=True, log_label='总用时'):
        """
        :param commit: 是否在最后提交事务(设置为False的时候方便单元测试)
        :param log_time:  是否打印程序运行总时间
        :param log_label:  自定义log的文字
        """
        self._log_time = log_time
        self._commit = commit
        self._log_label = log_label
    def __enter__(self):
        # 如果需要记录时间
        if self._log_time is True:
            self._start = default_timer()
        # 从连接池获取数据库连接
        conn = mysqlpool.get_conn()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        conn.autocommit = False
        self._conn = conn
        self._cursor = cursor
        return self
    def __exit__(self, *exc_info):
        # 提交事务
        if self._commit:
            self._conn.commit()
        # 在退出的时候自动关闭连接和cursor
        self._cursor.close()
        self._conn.close()
        if self._log_time is True:
            diff = default_timer() - self._start
            print('-- %s: %.6f 秒' % (self._log_label, diff))


DB_PARAMS = dict(
	host='10.4.56.9',
	port=13306,
	db='dbtest',
	user='root',
	passwd='ADSFabc@!#123456**22',
	charset='utf8mb4',
	connect_timeout=10,
)

class PooledConnection(object):
    def __init__(self, maxconnections):
        self._pool = Queue(maxconnections)  # create the queue
        self.maxconnections = maxconnections
        try:
            for i in range(maxconnections):
                self.fillConnection(self.CreateConnection())
        except Exception as e:
            print('111: %s' % str(e))
    def fillConnection(self, conn):
        try:
            self._pool.put(conn)
        except Exception as e:
            raise "fillConnection error: %s" % str(e)
    def CreateConnection(self):
        try:
            # conndb=mysqldb.connect(db=conf.mydb,host=conf.dbip,user=conf.myuser,passwd=conf.mypasswd);
            conndb = pymysql.connect(**DB_PARAMS)
            conndb.clientinfo = 'datasync connection pool from datasync.py'
            conndb.ping()
            return conndb
        except Exception as e:
            print('conn targetdb datasource Excepts!!!(%s).' % str(e))
            return None
    def colseConnection(self, conn):
        try:
            self._pool.get().close()
            self.fillConnection(self.CreateConnection())
        except Exception as e:
            raise ("CloseConnection error:" + str(e))
    def getConnection(self, timeout=1):
        try:
            return self._pool.get(timeout=timeout)
        except Exception as e:
            raise "getConnection error: %s" % str(e)
    def returnConnection(self, conn, timeout=1):
        try:
            self._pool.put(conn, timeout=timeout)
        except Exception as e:
            raise "returnConnection error: %s" % str(e)
    def __str__(self):
        return ""

mysqlpool = PooledConnection(10)

if __name__ == "__main__":
    print('1111111111')
    c = MysqlPool()
    print(c._MysqlPool__pool)
    sql1 = 'SELECT * FROM  aaa'
    result1 = c.select_many(sql1)
    print(result1)