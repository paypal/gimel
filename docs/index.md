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
| <img src="images/scala.png" width="90" height="40" /> | 2.12.10 | PRODUCTION | | <br> Data API is built on scala 2.12.10 <br> regardless the library should be compatible as long as the spark major version of library and the environment match <br> |
| <img src="images/python.png" width="100" height="40" /> | 3x | PRODUCTION | [PySpark Support](getting-started/gimel-pyspark-support.md)  | Data API works fully well with PySpark as long as spark version in environment & Gimel library matches. |
| <img src="images/spark.png" width="90" height="40" /> | 2.4.7 | PRODUCTION | | This is the recommended version |
| <img src="images/hadoop.png" width="120" height="40" /> | 2.10.0 | PRODUCTION | | This is the recommended version |
| <img src="images/s3.png" width="100" height="100" /> | 1.10.6 | PRODUCTION | [S3 Doc](gimel-connectors/s3.md) | |
| <img src="images/big-query.png" width="150" height="75" /> | 0.17.3 | PRODUCTION | [Big Query Doc](gimel-connectors/big-query.md) | |
| <img src="images/teradata.png" width="120" height="40" /> | 14 | PRODUCTION | [Teradata Doc](gimel-connectors/teradata.md) | Uses JDBC Connector internally<br> |
| <img src="images/hive.png" width="80" height="60" /> | 2.3.7 | PRODUCTION | [Hive Doc](gimel-connectors/hive.md) | |
| <img src="images/kafka.png" width="100" height="40" /> | 2.1.1 | PRODUCTION | [Kafka 2.2 Doc](gimel-connectors/kafka2.md) | V2.1.1 is the PayPal's Supported Version of Kafka|
| <img src="images/sftp.png" width="120" height="40" /> | 0.82 | PRODUCTION | [SFTP Doc](gimel-connectors/sftp.md) | Read/Write files from/To SFTP server |
| <img src="images/elasticsearch.png" width="120" height="60" /> | 6.2.1 | PRODUCTION | [ElasticSearch Doc](gimel-connectors/elasticsearch.md)| |
| <img src="images/restapi.png" width="150" height="60" /> | NA | PRODUCTION WITH LIMITATIONS | [Restful/Web-API Doc](gimel-connectors/restapi.md) | <br>Allows Accessing Data<br>- to any source supporting<br>- Rest API<br> |
| <img src="images/aerospike.png" width="120" height="40" /> | 3.1.5 | EXPERIMENTAL | [Aerospike Doc](gimel-connectors/aerospike.md) | Experimental API for Aerospike reads / writes |
| <img src="images/cassandra.png" width="100" height="60" /> | 2.0 | EXPERIMENTAL | [Cassandra Doc](gimel-connectors/cassandra.md) | <br>Experimental API for Cassandra reads / writes<br>Leverages DataStax Connector<br> |
| <br>Gimel Serde<br> | 1.0 | PRODUCTION | [Gimel Serde Doc](gimel-serde/gimel-serde.md) | Pluggable gimel serializers and deserializers |

_________________________________________________________________________________________


# Questions

  * [Slack](https://gimel-dev.slack.com)
  * [User Forum](https://groups.google.com/d/forum/gimel-user)
  * [Developer Forum](https://groups.google.com/d/forum/gimel-dev)
