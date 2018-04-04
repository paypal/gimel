# <img src="docs/images/gimel.png" width="60" height="60" /> Gimel Data API


Gimel provides unified Data API to access data from any storage like HDFS, GS, Alluxio, Hbase, Aerospike, BigQuery, Druid, Elastic, Teradata, Oracle, MySQL, etc.



--------------------------------------------------------------------------------------------------------------------


Contents
=================

  * [APIs & Version Compatibility](#stack-&-version-compatibility)
  * [Getting Started](docs/getting-started.md)
  * [Gimel Catalog Providers](docs/gimel-catalog/catalog-provider.md)
  * [Contribution Guidelines](CONTRIBUTING.md)
  * [Questions](#questions)


--------------------------------------------------------------------------------------------------------------------


# Stack & Version Compatibility

|    Compute/Storage      | Version | Grade | Documentation | Notes |
| ------------- | ----------- | ------------ | ------------- |-----------------|
| <img src="docs/images/spark.png" width="90" height="40" /> | 2.2.0 | | | This is the recommended version |
| <img src="docs/images/hadoop.png" width="120" height="40" /> | 2.7.3 | | | This is the recommended version |
| <img src="docs/images/csv.png" width="60" height="60" /> | 2.7.3 | PRODUCTION | [CSV Reader Doc](docs/gimel-connectors/hdfs-csv.md) | CSV Reader & Writer for HDFS |
| <img src="docs/images/alluxio.png" width="120" height="40" /> | 2.7.3 | PRODUCTION WITH LIMITATIONS | [Cross-Cluster Doc](docs/gimel-connectors/hdfs-crosscluster.md) | <br>Allows Accessing Data<br>- Across Clusters<br>- Allxio<br> |
| <img src="docs/images/kafka.png" width="100" height="40" /> | 0.10.2 | PRODUCTION | [Kafka Doc](docs/gimel-connectors/kafka.md) | V0.10.2 is the PayPal's Supported Version of Kafka|
| <img src="docs/images/hbase.png" width="100" height="35" />  | 1.2 | PRODUCTION WITH LIMITATIONS | [HBASE Doc](docs/gimel-connectors/hbase.md) | Leverages SHC Connector internally & also supports Batch/Get/Puts |
| <img src="docs/images/aerospike.png" width="120" height="40" /> | 3.14 | EXPERIMENTAL | [Aerospike Doc](docs/gimel-connectors/aerospike.md) | Experimental API for Aerospike reads / writes |
| <img src="docs/images/cassandra.png" width="100" height="60" /> | 2.0 | EXPERIMENTAL | [Cassandra Doc](docs/gimel-connectors/cassandra.md) | <br>Experimental API for Cassandra reads / writes<br>Leverages DataStax Connector<br> |
| <img src="docs/images/elasticsearch.png" width="120" height="60" /> | 5.6.4 | PRODUCTION | [ElasticSearch Doc](docs/gimel-connectors/elasticsearch.md)| Has Special Support for PayPal's Daily ES indexes |
| <img src="docs/images/hive.png" width="80" height="60" /> | 1.2 | PRODUCTION | [Hive Doc](docs/gimel-connectors/hive.md) | |
| <img src="docs/images/teradata.png" width="120" height="40" /> | 1.6.2 | EXPERIMENTAL | [Teradata Doc](docs/gimel-connectors/teradata.md) | <br>EXPERIMENTAL API Only<br>Uses JDBC Connector internally<br> |
| <img src="docs/images/druid.png" width="120" height="40" /> | 0.82 | PRODUCTION | [Druid Doc](docs/gimel-connectors/druid.md) | Only Writes(Non-Batch Mode) |
| <img src="docs/images/gimel.png" width="60" height="60" /> <img src="docs/images/sql.png" width="60" height="60" /> | 1.0 | PRODUCTION | [GSQL Doc](docs/gimel-sql/gimel-sql.md) | Refer link for using GSQL (Gimel SQL) API |
| <img src="docs/images/gimel.png" width="60" height="60" /> <br>TestSuite <br> | 1.0 | PRODUCTION | [Test-Suite Doc](docs/gimel-testsuite/testsuite.md) | Current Implementation works with CataLog Provider - Hive |
| <br>Gimel Logging<br> | 0.4.2 | PRODUCTION | [Gimel Logging Doc](docs/gimel-logging/gimel-logging.md) | This is the Gimel Logging Framework |

_________________________________________________________________________________________


# Questions

| Where | Contact |
| -------- | -------- |
| Slack | https://gimel-dev.slack.com |
| User Forum | https://groups.google.com/d/forum/gimel-dev |
| Developer Forum | https://groups.google.com/d/forum/gimel-user |


