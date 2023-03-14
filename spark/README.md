
<div align="center">
    <h1>spark on kubernetes</h1>
</div>

## `conf/spark-defaults.conf`配置

```
spark.master=k8s\://https\://10.0.0.1\:443
spark.eventLog.enabled=true              # 开启事件日志
spark.eventLog.dir=file\:///tmp/spark-events  # 指定事件日志目录

spark.driver.log.dfsDir=/tmp/spark-events    # 持久化 spark dirver 日志
spark.driver.log.persistToDfs.enabled=true

spark.kubernetes.driver.volumes.persistentVolumeClaim.data.options.claimName=spark-history-pv-claim
spark.kubernetes.driver.volumes.persistentVolumeClaim.data.mount.path=/tmp/spark-events
spark.kubernetes.driver.volumes.persistentVolumeClaim.data.mount.readOnly=false

# 持久化工作目录 通过spark.kubernetes.file.upload.path 指定该目录
spark.kubernetes.driver.volumes.persistentVolumeClaim.upload.options.claimName=spark-source-dirs
spark.kubernetes.driver.volumes.persistentVolumeClaim.upload.mount.path=/opt/spark/work-dir
spark.kubernetes.driver.volumes.persistentVolumeClaim.upload.mount.readOnly=false
spark.kubernetes.executor.volumes.persistentVolumeClaim.upload.options.claimName=spark-source-dirs
spark.kubernetes.executor.volumes.persistentVolumeClaim.upload.mount.path=/opt/spark/work-dir
spark.kubernetes.executor.volumes.persistentVolumeClaim.upload.mount.readOnly=false

# 资产限制
spark.kubernetes.driver.request.cores=1
spark.kubernetes.driver.limit.cores=1
spark.kubernetes.executor.request.cores=500m
spark.kubernetes.executor.limit.cores=500m

# 调度到指定节点
spark.kubernetes.node.selector.kubernetes.io/hostname=node01

# 指定spark driver 为固定端口
spark.driver.port=30020

# Spark在shuffle和其他操作期间使用卷溢出数据 本地化
spark.kubernetes.driver.volumes.hostPath.spark-local-dir-1.options.path=/tmp/aaa
spark.kubernetes.driver.volumes.hostPath.spark-local-dir-1.mount.path=/var/data
spark.kubernetes.driver.volumes.hostPath.spark-local-dir-1.mount.readOnly=false
```


## 提交模式

#####  交互模式

```
echo -e "this is a test\n\rhello world\n\raa bb" > test.txt
/opt/spark/bin/pyspark \
--conf spark.kubernetes.container.image=apache/spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1  \
--conf spark.files=test.txt \   # 指定上传的测试文件
```

####  cluster 模式

```
./bin/spark-submit \
--deploy-mode cluster \
--conf spark.kubernetes.container.image=apache/spark-py:v3.3.2 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.namespace=spark  \
--conf spark.executor.instances=1  \
--conf spark.kubernetes.file.upload.path=/opt/spark/work-dir  \   # 将pi.py 文件上传到改目录下
work-dir/spark_df_01.py
# 通过kubectl logs -f XXX-driver  查看日志输出
```


#### db
启动shell时指定链接数据的jar包,或者放到spark的工作目录下的jars目录

```
/opt/spark/bin/pyspark --driver-class-path=/opt/spark/work-dir/mysql-connector-j.jar  --jars /opt/spark/work-dir/mysql-connector-j.jar  ...
```
