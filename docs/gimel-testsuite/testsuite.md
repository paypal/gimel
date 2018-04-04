
## Note

Test Suite Helps validate sanity of API & version compatibility with cluster
For every new storage we support in Data API, adding a Storage Validation in the module gimel-testsuite is recommended.
The TestSuite can be scheduled to run on the cluster(s) regulary to smoke test and capture early - any issues in the dev cycles.


## Buid Gimel

Refer [Build Gimel](docs/build-gimel.md)

## Executing Test Suite via spark-submit


```bash

export spark_master=yarn-client

#-------- Required for spark-submit
export SPARK_DIST_CLASSPATH=$(find /usr/hdp/current/hbase-client/lib/ -type f -name "*.jar" | tr -s '\n' ':')/etc/hbase/conf:/etc/hbase/conf/hbase-site.xml:/etc/spark/conf:/etc/spark/conf/hive-site.xml

#--------Set App Name
export app_name="TestSuite"

# choose the storage to test
export storages=kafka_avro,kafka_binary,kafka_string,hbase,hive,teradata_jdbc,teradata_bulkload,elasticsearch,kafka_stream


export ADDITIONAL_JARS=hdfs:///tmp/hive-jdbc-2.0.0.jar,hdfs:///tmp/qubole-hive-JDBC-final-0.0.7.jar,hdfs:///tmp/tdgssconfig.jar,hdfs:///tmp/terajdbc4.jar

export args="gimel.hbase.namespace.name=testsuite \
gimel.zookeeper.host=zk_host:2181 \
gimel.confluent.schema.url=http://confluent_schema_registry_host:8081 \
gimel.kafka.broker=kafka_broker:9092 \
gimel.kafka.consumer.checkpoint.root=/pcatalog/kafka_consumer/checkpoint/ \
es.nodes=http://elastic_host \
es.port=8080 \
smoketest.kafka.hive.table=pc_smoketest_kafka_only \
smoketest.kafka.topic=pc_smoke_test_topic \
smoketest.es.hive.table=pc_smoketest_es \
smoketest.es.index=pc_smoke_test_index_1 \
smoketest.hive.table=pc_smoketest_hive \
smoketest.hbase.hive.table=pc_smoketest_hbase \
smoketest.hbase.namespace=testsuite \
smoketest.hbase.table=pc_smoketest_hbase \
smoketest.hbase.table.row.key=id \
smoketest.hbase.table.columnfamily=personal,professional \
smoketest.hbase.table.columns=rowKey:id|personal:name,age,address|professional:company,designation,salary \
smoketest.gimel.hive.db=pcatalog_smoke_test \
smoketest.gimel.hive.location=/tmp/pcatalog \
smoketest.gimel.sample.rows.count=1000 \
smoketest.result.stats.es.host=http://elastic_host \
smoketest.result.stats.es.port=8080 \
smoketest.result.stats.es.index=pcatalog_smoketest_result \
smoketest.hbase.site.xml.hdfs=hdfs:///tmp/hbase-site.xml \
smoketest.hbase.site.xml.local=/tmp/hbase-site.xml \
smoketest.kafka.stream.hive.table=pc_smoketest_kafka_streaming \
smoketest.kafka.stream.es.hive.table=pc_smoketest_kafka_streaming_es_1 \
smoketest.kafka.stream.es.index=pc_smoketest_kafka_streaming_es_index_1 \
smoketest.kafka.stream.batch.interval=30 \
smoketest.gimel.streaming.awaitTerminationOrTimeout=60000 \
storages=${storages} \
post.smoketest.result=false \
gimel.cluster=$CLUSTER \
smoketest.teradata.table=pc_smoketest_teradata \
smoketest.teradata.db=scratch_db \
smoketest.gimel.hive.table=pc_smoktest_teradata_hive \
smoketest.teradata.username=$USER \
smoketest.teradata.password.file=hdfs:///user/$USER/password/teradata/pass.dat \
smoketest.teradata.url=teradata_host \
smoketest.teradata.write.type=FASTLOAD \
smoketest.teradata.read.type=FASTEXPORT \
smoketest.teradata.sessions=8 \
smoketest.teradata.batchsize=10000 \
pcatalog.use.kerberos=true gimel.keytab=$USER.keytab gimel.principal=$USER gimel.hiveJars.ddl=$ADDITIONAL_JARS"

export jarlocal=gimel-testsuite.jar

#Launch Job via SQL API
spark-submit \
--name "$app_name" \
--master yarn \
--deploy-mode client \
--class com.paypal.gimel.testsuite.TestSuite \
--conf "spark.dynamicAllocation.enabled=true" \
--conf "spark.shuffle.service.enabled=true" \
--conf "spark.dynamicAllocation.initialExecutors=5" \
--conf "spark.dynamicAllocation.minExecutors=2" \
--conf "spark.dynamicAllocation.maxExecutors=400" \
--conf "spark.dynamicAllocation.executorIdleTimeout=300s" \
--keytab ~/$USER.keytab \
--principal $USER \
--jars $ADDITIONAL_JARS \
$jarlocal \
$args
```