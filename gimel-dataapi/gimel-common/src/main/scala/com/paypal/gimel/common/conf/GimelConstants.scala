/*
 * Copyright 2018 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.gimel.common.conf

import scala.util.matching.Regex

object GimelConstants {

  // COMMON CONSTANTS USED ACROSS ENTIRE GIMEL
  val USER: String = "USER"
  val USER_NAME = "username"
  val HOST_NAME = "HOSTNAME"
  val EXIT_CONDITION = "exitCondition"
  val RESOLVED_HIVE_TABLE: String = "resolvedHiveTable"
  val GIMEL_PROPERTIES_FILE_KEY: String = "gimel.property.file"
  val GIMEL_PROPERTIES_FILE_NAME = "/gimel.properties"
  val DATASET_PROPS: String = "dataSetProperties"
  val APP_NAME: String = "appName"
  val APP_ID: String = "appId"
  val APP_TAG: String = "appTag"
  val DATASET: String = "dataSet"
  val KEY_TAB: String = "gimel.keytab"
  val KEY_TAB_PRINCIPAL: String = "gimel.principal"
  val SECURITY_AUTH: String = "hadoop.security.authentication"
  val SIMPLE: String = "simple"
  val KERBEROS: String = "kerberos"
  val DEFAULT_SECURITY_AUTH: String = SIMPLE
  val CLUSTER: String = "gimel.cluster"
  val DEPLOYMENT_CLUSTERS: String = "gimel.dataset.deployment.clusters"
  val STORAGE_TYPE: String = "gimel.storage.type"
  val LOG_LEVEL: String = "gimel.logging.level"
  val DATA_CACHE_IS_ENABLED: String = "gimel.dataset.caching.enabled"
  val DATA_CACHE_IS_ENABLED_FOR_ALL: String = "gimel.dataset.caching.enabled.for.all"
  val MAX_RESULTS_TO_SHOW: String = "gimel.query.results.show.rows.threshold"
  val SHOW_ROWS_ENABLED: String = "gimel.query.results.show.rows.only"
  val NONE_STRING: String = "NONE"
  val DEFAULT_STRING: String = "default"
  val NULL_STRING: String = "null"
  val PCATALOG_STRING: String = "pcatalog"
  val UDC_STRING: String = "udc"
  val STORAGE_HANDLER: String = "storage_handler"
  val TBL_PROPERTIES: String = "TBLPROPERTIES"
  val DEFAULT_LOG_LEVEL: String = "INFO"
  val CREATE_STATEMENT_IS_PROVIDED: String = "gimel.sql.create.statement.is.provided"
  val TABLE_FILEDS: String = "gimel.table.fields"
  val TABLE_SQL: String = "gimel.table.sql"

  // HTTP status codes
  val HTTP_SUCCESS_STATUS_CODE: Int = 200
  val HTTP_SUCCESS_RESPONSE_CODE: Int = 300

  // Special characters
  val COMMA: String = ","
  val DOT: String = "."
  val SEMI_COLON: String = ";"
  val COLON: String = ":"
  val NEW_LINE: String = "\n"
  val SPACE_CHAR: String = " "

  // Common string constants
  val SUCCESS = "success"
  val FAILURE = "failure"
  val EMPTY_STRING = ""
  val READ_OPERATION = "read"
  val WRITE_OPERATION = "write"
  val ONE_BIGINT: BigInt = BigInt(1)
  val UNKNOWN_STRING = "unknown"

  // HIVE
  val HIVE_DATABASE_NAME: String = "gimel.hive.db.name"
  val HIVE_TABLE_NAME: String = "gimel.hive.table.name"
  val STORAGE_TYPE_HIVE: String = "hive"
  val HIVE_DDL_PARTITIONED_BY_CLAUSE: String = "PARTITIONED"
  val HIVE_DDL_PARTITIONS_STR: String = "PARTITIONS"

  // ELASTIC SEARCH CONSTANTS USED ACROSS ENTIRE GIMEL
  val ES_POLLING_STORAGES: String = "gimel.es.polling"
  val ES_NODE: String = "es.nodes"
  val ES_PORT: String = "es.port"
  val ES_URL_WITH_PORT: String = "gimel.es.elasticadpcluster.url"

  // HBASE CONSTANTS USED ACROSS ENTIRE GIMEL
  val HBASE_NAMESPACE: String = "gimel.hbase.namespace.name"
  val STORAGE_TYPE_HBASE: String = "hbase"
  val HBASE_PAGE_SIZE = "spark.hbase.connector.pageSize"

  // JDBC RELATED CONSTANTS
  val STORAGE_TYPE_JDBC = "JDBC"
  val GIMEL_JDBC_OPTION_KEYWORD: String = "gimel.jdbc."
  val JDBC_CHARSET_KEY: String = "charset"

  // TERA DATA RELATED CONSTANTS
  val TERA_DATA_TABLE_TYPE_COLUMN: String = "table_type"
  val TERA_DATA_INDEX_NAME_COLUMN: String = "index_name"
  val TERA_DATA_INDEX_COLUMN: String = "index_column"
  val TERADATA_TABLE_IDENTIFIER: String = "udc.teradata"
  val TERADATA_JSON_COLUMN_TYPE: String = "JSON"
  val EXPLAIN_CONTEXT: String = "explain"
  val ORACLE_EXPLAIN_CONTEXT: String = s"$EXPLAIN_CONTEXT plan for"
  val ROWS_IDENTIFIER: Regex = "([\\d,]+) row".r
  val SPACE_IDENTIFIER: Regex = "([\\d,]+) byte".r
  val CONFIDENCE_IDENTIFIER: Regex = "(\\S+)\\s* confidence".r
  val LOW_CONFIDENCE_IDENTIFIER: String = "low"
  val HIGH_CONFIDENCE_IDENTIFIER: String = "high"
  val NO_CONFIDENCE_IDENTIFIER: String = "no"
  val INDEX_JOIN_CONFIDENCE_IDENTIFIER: String = "index join"

  trait ConfidenceIdentifier {
    def identifier: String = ""
  }

  case object NoConfidence extends ConfidenceIdentifier {
    override def identifier: String = NO_CONFIDENCE_IDENTIFIER
  }

  case object LowConfidence extends ConfidenceIdentifier {
    override def identifier: String = LOW_CONFIDENCE_IDENTIFIER
  }

  case object HighConfidence extends ConfidenceIdentifier {
    override def identifier: String = HIGH_CONFIDENCE_IDENTIFIER
  }

  case object IndexJoinConfidence extends ConfidenceIdentifier {
    override def identifier: String = INDEX_JOIN_CONFIDENCE_IDENTIFIER
  }

  // spark configs
  val SPARK_SPECULATION = "spark.speculation"
  val SPARK_EXECUTOR_MEMORY: String = "spark.executor.memory"
  val SPARK_DRIVER_MEMORY: String = "spark.driver.memory"
  val SPARK_EXECUTOR_INSTANCES: String = "spark.executor.instances"
  val SPARK_DRIVER_CORES: String = "spark.driver.cores"
  val SPARK_EXECUTOR_CORES: String = "spark.executor.cores"
  val SPARK_APP_NAME: String = "spark.app.name"
  val SPARK_APP_ID: String = "spark.app.id"

  // KAFKA CONSTANTS USED ACROSS ENTIRE GIMEL
  val ZOOKEEPER_LIST: String = "gimel.zookeeper.host"
  val ZOOKEEPER_STATE: String = "gimel.zk.state.prefix"
  val CONFLUENT_SCHEMA_URL: String = "gimel.kafka.confluent.schema.url"
  val KAFKA_CDH_SCHEMA: String = "gimel.kafka.cdh.coreSchema"
  val KAFKA_CONSUMER_CHECKPOINT_PATH: String = "gimel.kafka.consumer.checkpoint.root"
  val KAFKA_BROKER_LIST: String = "gimel.kafka.broker"
  val CDH_BROKER_LIST: String = "gimel.kafka.cdh.broker"
  val STREAM_MODE: String = "gimel.kafka.stream.mode"
  val DS_STREAM: String = "direct.stream"
  val BATCH_INTERVAL: String = "15"
  val GIMEL_KAFKA_VERSION = "gimel.kafka.api.version"
  val GIMEL_KAFKA_VERSION_ONE = "1"
  val GIMEL_KAFKA_VERSION_TWO = "2"
  val GIMEL_KAFKA_DEFAULT_VERSION: String = GIMEL_KAFKA_VERSION_TWO

  // Gimel Streaming
  val GIMEL_STREAMING_CHECKPOINT_LOCATION = "gimel.streaming.checkpoint.location"
  val GIMEL_STREAMING_OUTPUT_MODE = "gimel.streaming.output.mode"
  val STREAMING_CHECKPOINT_LOCATION = "checkpointLocation"
  val GIMEL_STREAMING_TRIGGER_INTERVAL = "gimel.streaming.trigger.interval"

  // hdfs constants
  val HDFS_IMPL = "fs.hdfs.impl"
  val FILE_IMPL = "fs.file.impl"
  val DEFAULT_FILE_SYSTEM = "fs.defaultFS"
  val LOCAL_FS = "org.apache.hadoop.fs.LocalFileSystem"
  val DISTRIBUTED_FS = "org.apache.hadoop.hdfs.DistributedFileSystem"
  val HADDOP_FILE_SYSTEM = "hdfs"
  val LOCAL_FILE_SYSTEM = "local"
  val FS_DEFAULT_NAME: String = "fs.default.name"
  val hdfsStorageNameKey = "gimel.hdfs.storage.name"
  val hdfsNameNodeKey = "gimel.hdfs.nn"

  // CONDITIONAL FLAGS
  val FALSE = "false"

  // Connection timeout for requests in seconds
  val CONNECTION_TIMEOUT = 180

  // GTS
  val GTS_DEFAULT_USER = ""
  val GTS_USER_CONFIG = "gimel.gts.user"
  val GTS_IMPERSONATION_FLAG = "spark.gimel.gts.impersonation.enabled"

  // Serialization/Deserialization Class
  val GIMEL_DESERIALIZER_CLASS = "gimel.deserializer.class"
  val GIMEL_SERIALIZER_CLASS = "gimel.serializer.class"
}


