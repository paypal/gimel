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

package com.paypal.gimel.hbase.utilities

import java.io.File

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.Files
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.hbase.SparkHBaseConf
import org.apache.spark.sql.util._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import com.paypal.gimel.common.catalog.Field
import com.paypal.gimel.hbase.DataSet

class HBaseLocalClient extends FunSuite with Matchers with BeforeAndAfterAll {

  var sparkSession : SparkSession = _
  var dataSet: DataSet = _
  val hbaseTestingUtility = new HBaseTestingUtility()
  val tableName = "test_table"
  val cfs = Array("personal", "professional")
  val columns = Array("id", "name", "age", "address", "company", "designation", "salary")
  val fields = columns.map(col => new Field(col))

  val metrics = ArrayBuffer.empty[(String, QueryExecution, Long)]

  protected override def beforeAll(): Unit = {
    val tempDir: File = Files.createTempDir
    tempDir.deleteOnExit
    hbaseTestingUtility.startMiniCluster()
    SparkHBaseConf.conf = hbaseTestingUtility.getConfiguration
    createTable(tableName, cfs)
    val conf = new SparkConf
    conf.set(SparkHBaseConf.testConf, "true")
    sparkSession = SparkSession.builder()
      .master("local")
      .appName("HBase Test")
      .config(conf)
      .getOrCreate()

    val listener = new QueryExecutionListener {
      // Only test successful case here, so no need to implement `onFailure`
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        metrics += ((funcName, qe, duration))
      }
    }
    sparkSession.listenerManager.register(listener)
    sparkSession.sparkContext.setLogLevel("ERROR")
    dataSet = new DataSet(sparkSession)
  }

  protected override def afterAll(): Unit = {
    hbaseTestingUtility.shutdownMiniCluster()
    sparkSession.close()
  }

  def createTable(name: String, cfs: Array[String]) {
    val tName = Bytes.toBytes(name)
    val bcfs = cfs.map(Bytes.toBytes(_))
    try {
      hbaseTestingUtility.deleteTable(TableName.valueOf(tName))
    } catch {
      case _ : Throwable =>
        println("No table = " + name + " found")
    }
    hbaseTestingUtility.createMultiRegionTable(TableName.valueOf(tName), bcfs)
  }

  // Mocks data for testing
  def mockDataInDataFrame(numberOfRows: Int): DataFrame = {
    def stringed(n: Int) = s"""{"id": "$n","name": "MAC-$n", "address": "MAC-${n + 1}", "age": "${n + 1}", "company": "MAC-$n", "designation": "MAC-$n", "salary": "${n * 10000}" }"""
    val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }
    val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
    val dataFrame: DataFrame = sparkSession.read.json(rdd)
    dataFrame
  }
}
