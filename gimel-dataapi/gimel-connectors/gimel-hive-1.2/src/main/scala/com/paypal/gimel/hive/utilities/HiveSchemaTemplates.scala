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

package com.paypal.gimel.hive.utilities

object HiveSchemaTemplates {

  // This is the Schema Template for
  // Hive External Table

  private val templateRFDTable =
    """
      |CREATE ${varTableType} TABLE IF NOT EXISTS ${varDatabaseName}.${varTableName}
      |(
      |${varColumnSet}
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ‘${varFieldDelimiter}’
      |LINES TERMINATED BY ‘${varRowDelimiter}’
      |STORED AS ${varStorageFormat}
      |LOCATION '${varLocation}'
      |""".stripMargin

  // This is the Schema Template for
  // Row Format Delimited Table that is
  // Partitioned

  private val templateRFDTPartitionedTable =
    """
      |CREATE ${varTableType} TABLE IF NOT EXISTS ${varDatabaseName}.${varTableName}
      |(
      |${varColumnSet}
      |)
      |PARTITIONED BY
      |(
      |${varPartitionColumnSet}
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ‘${varFieldDelimiter}’
      |LINES TERMINATED BY ‘${varRowDelimiter}’
      |STORED AS ${varStorageFormat}
      |LOCATION '${varLocation}'
      |""".stripMargin

  // This is the Schema Template for
  // Row Format Delimited Table that is
  // Partitioned and
  // Bucketed

  private val templateRFDTPartitionedBucketedTable =
    """
      |CREATE ${varTableType} TABLE IF NOT EXISTS ${varDatabaseName}.${varTableName}
      |(
      |${varColumnSet}
      |)
      |PARTITIONED BY
      |(
      |${varPartitionColumnSet}
      |)
      |CLUSTERED BY
      |(
      |${varClusterKey}
      |)
      |INTO ${varClusterCount} BUCKETS
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ‘${varFieldDelimiter}’
      |LINES TERMINATED BY ‘${varRowDelimiter}’
      |STORED AS ${varStorageFormat}
      |LOCATION '${varLocation}'
      |""".stripMargin

  // This is the Schema Template for
  // Hive External Table that is
  // Partitioned and
  // Bucketed

  private val templatePartitionedBucketedTable =
    """
      |CREATE ${varTableType} TABLE IF NOT EXISTS ${varDatabaseName}.${varTableName}
      |(
      |${varColumnSet}
      |)
      |PARTITIONED BY
      |(
      |${varPartitionColumnSet}
      |)
      |CLUSTERED BY
      |(
      |${varClusterKey}
      |)
      |INTO ${varClusterCount} BUCKETS
      |STORED AS ${varStorageFormat}
      |LOCATION '${varLocation}'
      |""".stripMargin

  // This is the Schema Template for
  // Hive External Table that is
  // Partitioned

  private val templatePartitionedTable =
    """
      |create ${varTableType} table IF NOT EXISTS ${varDatabaseName}.${varTableName}
      |(
      |${varColumnSet}
      |)
      |partitioned by
      |(
      |${varPartitionColumnSet}
      |)
      |stored as ${varStorageFormat}
      |location '${varLocation}'
      | """.stripMargin

  // This is the Schema Template for
  // Hive External Table that has no partitions or bucketing

  private val templateSimpleTable =
    """
      |CREATE ${varTableType} TABLE IF NOT EXISTS ${varDatabaseName}.${varTableName}
      |(
      |${varColumnSet}
      |)
      |STORED AS ${varStorageFormat}
      |LOCATION '${varLocation}'
      |""".stripMargin

  // In DDL namespace:table must always be specified to ensure Hive queries work.However, in the codebase for spark 1.6.2 , SHC connector expects hbase.table.name = "namespace:tablename"whereas in spark 2.1.1, SHC connector expects hbase.namespace & hbase.table.name separately.So logic is different the Gimel API codebase for 2.1.1 & 1.6.2 versions.

  private val templateSimpleHBaseTable =
    """
      |CREATE EXTERNAL TABLE IF NOT EXISTS ${varDatabaseName}.${varTableName}
      |(
      |${varColumnSet}
      |)
      |WITH SERDEPROPERTIES ("gimel.hbase.columns.mapping"="${varHBaseTableMapping}")
      |TBLPROPERTIES ("gimel.hbase.table.name" ="${varHBaseTableName}","gimel.hbase.namespace.name"="${varHBaseNamespace}");
      |""".stripMargin

  // simple Hbase Hive Table template without HbaseStorageHandler
  private val templateSimpleHBaseTableWithoutHandler =
  """
    |CREATE EXTERNAL TABLE IF NOT EXISTS ${varDatabaseName}.${varTableName}
    |(
    |${varColumnSet}
    |)
    |WITH SERDEPROPERTIES ("gimel.hbase.columns.mapping"="${varHBaseTableMapping}")
    |TBLPROPERTIES ("gimel.hbase.table.name" ="${varHBaseTableName}","gimel.hbase.namespace.name"="${varHBaseNamespace}",'gimel.storage.type'='HBASE');
    |""".stripMargin

  // This is the Schema Template for
  // Hive External Table that is created via org.apache.spark.sql.<serde>
  // <serde> being either parquet or json or anything new that could be supported in the future

  private val templateSerdeTable =
    """
      |create table IF NOT EXISTS ${varDatabaseName}.${varTableName}
      |using ${varSparkSQLSerde}
      |options (path "${varLocation}")
      |""".stripMargin


  val tableTemplates: Map[String, String] = Map(
    "templatePartitionedBucketedTable" -> templatePartitionedBucketedTable
    , "templatePartitionedTable" -> templatePartitionedTable
    , "templateSimpleTable" -> templateSimpleTable
    , "templateSerdeTable" -> templateSerdeTable
    , "templateRFDTable" -> templateRFDTable
    , "templateRFDTPartitionedTable" -> templateRFDTPartitionedTable
    , "templateRFDTPartitionedBucketedTable" -> templateRFDTPartitionedBucketedTable
    , "templateSimpleHBaseTable" -> templateSimpleHBaseTable
    , "templateSimpleHBaseTableWithoutHandler" -> templateSimpleHBaseTableWithoutHandler
  )
}
