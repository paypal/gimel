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

package com.paypal.gimel.hdfs

import org.apache.spark.sql.SparkSession
import org.scalatest._

import com.paypal.gimel.common.catalog.{DataSetProperties, Field}

class DataSetTest extends FunSpec with Matchers with BeforeAndAfter {

  var sparkSession : SparkSession = _

  var dataSet : DataSet = _

  before {
    sparkSession = SparkSession.builder().appName("Hdfs Test")
      .master("local")
      .getOrCreate()
    dataSet = new DataSet(sparkSession)
  }

  after {
    sparkSession.close()
  }

  it("should test json in Hdfs dataset read") {
    val resourcesPath = "file://" + (getClass.getResource("/hdfs_test.json")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location" -> resourcesPath,
      "gimel.hdfs.data.format" -> "json",
      "gimel.hive.db.name" -> "db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    val dataSetProperties = DataSetProperties("MyDataset", null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      "gimel.hdfs.csv.data.inferSchema" -> "false",
      "gimel.hdfs.csv.data.headerProvided" -> "true",
      "gimel.hdfs.column.delimiter" -> "\t")
    val df = dataSet.read("My DataSet", datasetProps)
    df.show
    assert(df.count() == 1)
  }

  it("should test csv in Hdfs dataset read") {
    val resourcesPath = "file://" + (getClass.getResource("/hdfs_test.csv")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location"-> resourcesPath,
      "gimel.hdfs.data.format"-> "csv",
      "gimel.hive.db.name"->"db",
      "gimel.hive.table.name"->"table",
      "gimel.hdfs.nn" -> "file:/")
    val dataSetProperties = DataSetProperties("MyDataset", null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties"->dataSetProperties,
      "gimel.hdfs.csv.data.headerProvided"->"true",
      "gimel.hdfs.column.delimiter"->",",
      "gimel.hdfs.read.options" -> "{\"gimel.hdfs.csv.data.inferSchema\" : \"true\", \"gimel.hdfs.csv.data.headerProvided\" : \"true\"}")
    val df = dataSet.read("My DataSet", datasetProps)
    df.show(100)
    assert(df.count() == 1)
  }

  it("should test txt in Hdfs dataset read") {
    val resourcesPath = "file://" + (getClass.getResource("/hdfs_test.txt")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location"-> resourcesPath,
      "gimel.hdfs.data.format"-> "text",
      "gimel.hdfs.nn" -> "file:/",
      "gimel.fs.column.delimiter"->",",
      "gimel.fs.row.delimiter"->"\n")
    val fields = Array(Field("Name", "String", true, false, 1),
      Field("Age", "String", true, false, 1),
      Field("Email", "String", true, false, 1))
    val dataSetProperties = DataSetProperties("MyDataset", fields, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties"->dataSetProperties,
      "gimel.hdfs.csv.data.inferSchema"->"false",
      "gimel.hdfs.csv.data.headerProvided"->"true")
    val df = dataSet.read("My DataSet", datasetProps)
    df.show()
    assert(df.count() == 1)
  }

  it("should Test avro in Hdfs dataset read") {
    val resourcesPath = "file://" + (getClass.getResource("/hdfs_test.avro")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location" -> resourcesPath,
      "gimel.hdfs.data.format" -> "avro",
      "gimel.hive.db.name"->"db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    val dataSetProperties = DataSetProperties("MyDataset", null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      "gimel.hdfs.csv.data.inferSchema" -> "false",
      "gimel.hdfs.csv.data.headerProvided" -> "true",
      "gimel.hdfs.column.delimiter" -> "\t")
    val df = dataSet.read("My DataSet", datasetProps)
    df.show
    assert(df.count() == 2)
  }

  it("should Test gz in Hdfs dataset read") {
    val resourcesPath = "file://" + (getClass.getResource("/hdfs_test.txt.gz")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location"-> resourcesPath,
      "gimel.hdfs.data.format" -> "gzip",
      "gimel.hive.db.name" -> "db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    val fields = Array(Field("value", "String", true, false, 1))
    val dataSetProperties = DataSetProperties("MyDataset", fields, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      "gimel.hdfs.csv.data.inferSchema" -> "false",
      "gimel.hdfs.csv.data.headerProvided" -> "true",
      "gimel.hdfs.column.delimiter" -> "\t")
    val df = dataSet.read("My DataSet", datasetProps)
    df.show
    assert(df.count() == 1)
  }

  it("should Test sequence files in Hdfs dataset read") {
    val resourcesPath = "file://" + (getClass.getResource("/hdfs_test.seq")).getPath
    val props : Map[String, String] = Map("gimel.hdfs.data.location" -> resourcesPath,
      "gimel.hdfs.data.format" -> "sequence",
      "gimel.hive.db.name" ->"db",
      "gimel.hive.table.name" -> "table",
      "gimel.hdfs.nn" -> "file:/")
    val fields = Array(Field("value", "String", true, false, 1))
    val dataSetProperties = DataSetProperties("MyDataset", fields, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      "gimel.hdfs.csv.data.inferSchema" -> "false",
      "gimel.hdfs.csv.data.headerProvided" -> "true",
      "gimel.hdfs.column.delimiter" -> "\t")
    val df = dataSet.read("My DataSet", datasetProps)
    df.show
    assert(df.count() == 100)
  }
}
