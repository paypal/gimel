# <img src="images/gimel.png" width="60" height="60" /> Gimel Data API


Gimel provides unified Data API to access data from any storage like HDFS, GS, Alluxio, Hbase, Aerospike, BigQuery, Druid, Elastic,  Teradata, Oracle, MySQL, SFTP, etc.


--------------------------------------------------------------------------------------------------------------------


Contents
=================

  * [Edit on GitHub](https://github.com/paypal/gimel)
  * [APIs & Version Compatibility](#stack-&-version-compatibility)
  * [Getting Started](getting-started/build-gimel.md)
  * [Gimel Catalog Providers](gimel-catalog/catalog-provider.md)
  * [Contribution Guidelines](CONTRIBUTING.md)
  * [Adding a new connector](gimel-connectors/adding-new-connector.md)
  * [Questions](#questions)

--------------------------------------------------------------------------------------------------------------------


# Stack & Version Compatibility

|    Compute/Storage/Language      | Version | Grade | Documentation | Notes |
| ------------- | ----------- | ------------ | ------------- |-----------------|
| <img src="images/scala.png" width="90" height="40" /> | 2.11.8 | PRODUCTION | | <br> Data API is built on scala 2.11.8 <br> regardless the library should be compatible as long as the spark major version of library and the environment match <br> |
| <img src="images/python.png" width="100" height="40" /> | 2.6.6 | PRODUCTION | [PySpark Support](getting-started/gimel-pyspark-support.md)  | Data API / GSQL works fully well with PySpark as long as spark version in environment & Gimel library matches. |
| <img src="images/spark.png" width="90" height="40" /> | 2.3.0 | | | This is the recommended version |
| <img src="images/hadoop.png" width="120" height="40" /> | 2.7.3 | | | This is the recommended version |
| <img src="images/csv.png" width="60" height="60" /> | 2.7.3 | PRODUCTION | [CSV Reader Doc](gimel-connectors/hdfs-csv.md) | CSV Reader & Writer for HDFS |
| <img src="images/restapi.png" width="150" height="60" /> | 2.7.3 | PRODUCTION WITH LIMITATIONS | [Restful/Web-API Doc](gimel-connectors/restapi.md) | <br>Allows Accessing Data<br>- to any source supporting<br>- Rest API<br> |
| <img src="images/alluxio.png" width="120" height="40" /> | 2.7.3 | PRODUCTION WITH LIMITATIONS | [Cross-Cluster Doc](gimel-connectors/hdfs-crosscluster.md) | <br>Allows Accessing Data<br>- Across Clusters<br>- Allxio<br> |
| <img src="images/kafka.png" width="100" height="40" /> | 0.10.2 | PRODUCTION | [Kafka Doc](gimel-connectors/kafka.md) | V0.10.2 is the PayPal's Supported Version of Kafka|
| <img src="images/hbase.png" width="100" height="35" />  | 1.2 | PRODUCTION WITH LIMITATIONS | [HBASE Doc](gimel-connectors/hbase.md) | Leverages SHC Connector internally & also supports Batch/Get/Puts |
| <img src="images/aerospike.png" width="120" height="40" /> | 3.14 | EXPERIMENTAL | [Aerospike Doc](gimel-connectors/aerospike.md) | Experimental API for Aerospike reads / writes |
| <img src="images/cassandra.png" width="100" height="60" /> | 2.0 | EXPERIMENTAL | [Cassandra Doc](gimel-connectors/cassandra.md) | <br>Experimental API for Cassandra reads / writes<br>Leverages DataStax Connector<br> |
| <img src="images/elasticsearch.png" width="120" height="60" /> | 5.6.4 | PRODUCTION | [ElasticSearch Doc](gimel-connectors/elasticsearch.md)| Has Special Support for PayPal's Daily ES indexes |
| <img src="images/hive.png" width="80" height="60" /> | 1.2 | PRODUCTION | [Hive Doc](gimel-connectors/hive.md) | |
| <img src="images/teradata.png" width="120" height="40" /> | 1.6.2 | EXPERIMENTAL | [Teradata Doc](gimel-connectors/teradata.md) | <br>EXPERIMENTAL API Only<br>Uses JDBC Connector internally<br> |
| <img src="images/druid.png" width="120" height="40" /> | 0.82 | PRODUCTION | [Druid Doc](gimel-connectors/druid.md) | Only Writes(Non-Batch Mode) |
| <img src="images/sftp.png" width="120" height="40" /> | 0.82 | PRODUCTION | [SFTP Doc](gimel-connectors/sftp.md) | Read/Write files from/To SFTP server |
| <img src="images/gimel.png" width="60" height="60" /> <img src="images/sql.png" width="60" height="60" /> | 1.0 | PRODUCTION | [GSQL Doc](gimel-sql/gimel-sql.md) | Refer link for using GSQL (Gimel SQL) API |
| <br>Gimel Logging<br> | 0.4.3 | PRODUCTION | [Gimel Logging Doc](gimel-logging/gimel-logging.md) | This is the Gimel Logging Framework |
| <br>Gimel Serde<br> | 1.0 | PRODUCTION | [Gimel Serde Doc](gimel-serde/gimel-serde.md) | Pluggable gimel serializers and deserializers |
| <br>Unified Data Catalog<br> | 0.0.1 | PRODUCTION | [UDC Doc](udc/README.md) | This is Unified Data Catalog|

_________________________________________________________________________________________


# Questions

  * [Slack](https://gimel-dev.slack.com)
  * [User Forum](https://groups.google.com/d/forum/gimel-user)
  * [Developer Forum](https://groups.google.com/d/forum/gimel-dev)
