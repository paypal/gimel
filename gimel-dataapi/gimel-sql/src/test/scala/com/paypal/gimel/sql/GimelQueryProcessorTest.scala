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

package com.paypal.gimel.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._

class GimelQueryProcessorTest extends FunSpec with Matchers with BeforeAndAfter {
  var sparkSession : SparkSession = _

  before {
    sparkSession = SparkSession.builder().appName("GimelQueryProcessor Test")
      .master("local")
      .getOrCreate()

    // HBaseLocalClient.startHbaseCluster(sparkSession)
  }

  after {
    sparkSession.close()
    // HBaseLocalClient.stopHbaseCluster()
  }

  /*
   * The test cases are in the ignored scope due to https://github.com/elastic/elasticsearch-hadoop/issues/1097
   * To test the following: Change "ignore" to "it"
   * Exclude either elasticsearch-hadoop pr elasticsearch-spark from the dependencies by changing their scope to provided in gimel-elasticsearch
   * OR Change the elasticsearch-hadoop version to 6.6.0 or 7.0.0
   */

  ignore("should test json in Hdfs dataset read via sql") {
    val gsql: String => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, sparkSession)
    val className = new com.paypal.gimel.hdfs.DataSet(sparkSession).getClass;
    val resourcesPath = "file://" + (className.getResource("/hdfs_test.json")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location" -> resourcesPath,
      "gimel.hdfs.data.format" -> "json",
      "gimel.hive.db.name" -> "db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    gsql("set gimel.catalog.provider=USER")
    val dataSetProperties = s"""
    {
      "datasetType" : "HDFS",
      "fields" : [],
      "partitionFields" : [],
      "props": {
        "gimel.hdfs.data.location" : "$resourcesPath",
        "gimel.hdfs.data.format" : "json",
        "gimel.hive.db.name" : "db",
        "gimel.hive.table.name" : "table",
        "gimel.hdfs.nn" : "file:/",
        "datasetName" : "MyDataset"
      }
    }"""
    gsql("set gimel.catalog.provider=USER")
    gsql(s"""set udc.hdfs.json.dataSetProperties=$dataSetProperties""")
    val res: DataFrame = gsql(s"""select * from udc.hdfs.json""")
    assert(res.count() == 1)
  }

  ignore("should test csv in Hdfs dataset read via sql") {
    val gsql: String => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, sparkSession)
    val className = new com.paypal.gimel.hdfs.DataSet(sparkSession).getClass;
    val resourcesPath = "file://" + (className.getResource("/hdfs_test.csv")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location" -> resourcesPath,
      "gimel.hdfs.data.format" -> "json",
      "gimel.hive.db.name" -> "db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    gsql("set gimel.catalog.provider=USER")
    val dataSetProperties = s"""
    {
      "datasetType" : "HDFS",
      "fields" : [],
      "partitionFields" : [],
      "props": {
        "gimel.hdfs.data.location" : "$resourcesPath",
        "gimel.hdfs.data.format" : "json",
        "gimel.hive.db.name" : "db",
        "gimel.hive.table.name" : "table",
        "gimel.hdfs.nn" : "file:/",
        "datasetName" : "MyDataset"
      }
    }"""
    gsql("set gimel.catalog.provider=USER")
    gsql(s"""set udc.hdfs.json.dataSetProperties=$dataSetProperties""")
    val res: DataFrame = gsql(s"""select * from udc.hdfs.json""")
    assert(res.count() == 1)
  }

  ignore("should test text in Hdfs dataset read via sql") {
    val gsql: String => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, sparkSession)
    val className = new com.paypal.gimel.hdfs.DataSet(sparkSession).getClass;
    val resourcesPath = "file://" + (className.getResource("/hdfs_test.txt")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location" -> resourcesPath,
      "gimel.hdfs.data.format" -> "json",
      "gimel.hive.db.name" -> "db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    gsql("set gimel.catalog.provider=USER")
    val dataSetProperties = s"""
    {
      "datasetType" : "HDFS",
      "fields" : [],
      "partitionFields" : [],
      "props": {
        "gimel.hdfs.data.location" : "$resourcesPath",
        "gimel.hdfs.data.format" : "json",
        "gimel.hive.db.name" : "db",
        "gimel.hive.table.name" : "table",
        "gimel.hdfs.nn" : "file:/",
        "datasetName" : "MyDataset"
      }
    }"""
    gsql("set gimel.catalog.provider=USER")
    gsql(s"""set udc.hdfs.json.dataSetProperties=$dataSetProperties""")
    val res: DataFrame = gsql(s"""select * from udc.hdfs.json""")
    assert(res.count()==1)
  }

  ignore("should test avro in Hdfs dataset read via sql") {
    val gsql: String => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, sparkSession)
    val className = new com.paypal.gimel.hdfs.DataSet(sparkSession).getClass;
    val resourcesPath = "file://" + (className.getResource("/hdfs_test.avro")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location" -> resourcesPath,
      "gimel.hdfs.data.format" -> "json",
      "gimel.hive.db.name" -> "db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    gsql("set gimel.catalog.provider=USER")
    val dataSetProperties = s"""
    {
      "datasetType" : "HDFS",
      "fields" : [],
      "partitionFields" : [],
      "props": {
        "gimel.hdfs.data.location" : "$resourcesPath",
        "gimel.hdfs.data.format" : "json",
        "gimel.hive.db.name" : "db",
        "gimel.hive.table.name" : "table",
        "gimel.hdfs.nn" : "file:/",
        "datasetName" : "MyDataset"
      }
    }"""
    gsql("set gimel.catalog.provider=USER")
    gsql(s"""set udc.hdfs.json.dataSetProperties=$dataSetProperties""")
    val res: DataFrame = gsql(s"""select * from udc.hdfs.json""")
    assert(res.count() == 2)
  }

  ignore("should test gz in Hdfs dataset read via sql") {
    val gsql: String => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, sparkSession)
    val className = new com.paypal.gimel.hdfs.DataSet(sparkSession).getClass;
    val resourcesPath = "file://" + (className.getResource("/hdfs_test.txt.gz")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location" -> resourcesPath,
      "gimel.hdfs.data.format" -> "json",
      "gimel.hive.db.name" -> "db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    gsql("set gimel.catalog.provider=USER")
    val dataSetProperties = s"""
    {
      "datasetType" : "HDFS",
      "fields" : [],
      "partitionFields" : [],
      "props": {
        "gimel.hdfs.data.location" : "$resourcesPath",
        "gimel.hdfs.data.format" : "json",
        "gimel.hive.db.name" : "db",
        "gimel.hive.table.name" : "table",
        "gimel.hdfs.nn" : "file:/",
        "datasetName" : "MyDataset"
      }
    }"""
    gsql("set gimel.catalog.provider=USER")
    gsql(s"""set udc.hdfs.json.dataSetProperties=$dataSetProperties""")
    val res: DataFrame = gsql(s"""select * from udc.hdfs.json""")
    assert(res.count() == 1)
  }

  ignore("should test sequence in Hdfs dataset read via sql") {
    val gsql: String => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, sparkSession)
    val className = new com.paypal.gimel.hdfs.DataSet(sparkSession).getClass;
    val resourcesPath = "file://" + (className.getResource("/hdfs_test.seq")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location" -> resourcesPath,
      "gimel.hdfs.data.format" -> "json",
      "gimel.hive.db.name" -> "db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    gsql("set gimel.catalog.provider=USER")
    val dataSetProperties = s"""
    {
      "datasetType" : "HDFS",
      "fields" : [],
      "partitionFields" : [],
      "props": {
        "gimel.hdfs.data.location" : "$resourcesPath",
        "gimel.hdfs.data.format" : "json",
        "gimel.hive.db.name" : "db",
        "gimel.hive.table.name" : "table",
        "gimel.hdfs.nn" : "file:/",
        "datasetName" : "MyDataset"
      }
    }"""
    gsql("set gimel.catalog.provider=USER")
    gsql(s"""set udc.hdfs.json.dataSetProperties=$dataSetProperties""")
    val res: DataFrame = gsql(s"""select * from udc.hdfs.json""")
    assert(res.count() == 100)
  }

//  ignore("should test hbase write") {
//    val tableName = "test_table"
//    val gsql: String => DataFrame = com.paypal.gimel.scaas.GimelQueryProcessor.executeBatch(_: String, sparkSession)
//    gsql("set " + HbaseConfigs.hbaseRowKey + "=id")
//    gsql("set " + HbaseConfigs.hbaseColumnMappingKey + "=personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary")
//    val dataFrame = HBaseLocalClient.mockDataInDataFrame(sparkSession, 1000)
//    dataFrame.registerTempTable("input_table")
//    val sql = "insert into HBase.Local.default." + tableName + " select * from input_table"
//    val df = gsql(sql)
//    df.show
//  }
//
//  ignore("should test hbase read with limit") {
//    val metrics = ArrayBuffer.empty[(String, QueryExecution, Long)]
//    val listener = new QueryExecutionListener {
//      // Only test successful case here, so no need to implement `onFailure`
//      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
//      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
//        metrics += ((funcName, qe, duration))
//      }
//    }
//    sparkSession.listenerManager.register(listener)
//    val tableName = "test_table"
//    val gsql: String => DataFrame = com.paypal.gimel.scaas.GimelQueryProcessor.executeBatch(_: String, sparkSession)
//    gsql("set " + HbaseConfigs.hbaseRowKey + "=id")
//    gsql("set " + HbaseConfigs.hbaseColumnMappingKey + "=personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary")
//    sparkSession.conf.set(GimelConstants.HBASE_PAGE_SIZE, 20)
//    val sql = "select * from HBase.Local.default." + tableName + " limit 20"
//    val df = gsql(sql)
//    df.show(20)
//    val metricInsertQuery = metrics(metrics.length - 1)
//    val qe = metricInsertQuery._2
//    println(qe.executedPlan.children(0).children(0).children(0).metrics)
//    val kafkaReadOutputRows = qe.executedPlan.children(0).children(0).children(0).metrics("numOutputRows").value
//    assert(kafkaReadOutputRows == 20)
//    sparkSession.conf.unset(GimelConstants.HBASE_PAGE_SIZE)
//
//  }

}
