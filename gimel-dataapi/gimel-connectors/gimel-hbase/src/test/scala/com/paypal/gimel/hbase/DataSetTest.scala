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

package com.paypal.gimel.hbase

import org.scalatest._

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.hbase.utilities.HBaseLocalClient

class DataSetTest extends HBaseLocalClient with Matchers {
  test("Write operation") {
    val props : Map[String, String] = Map(HbaseConfigs.hbaseNamespaceKey -> "default",
      HbaseConfigs.hbaseTableKey -> s"""$tableName""",
      HbaseConfigs.hbaseRowKey -> "id",
      HbaseConfigs.hbaseColumnMappingKey -> "personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary")
    val dataSetName = "HBase.Local.default." + tableName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties)
    val dataFrame = mockDataInDataFrame(10)
    dataFrame.show(1)
    val df = dataSet.write(dataSetName, dataFrame, datasetProps)
    assert(df.count() == 10)
  }

  test("Read operation") {
    val props : Map[String, String] = Map(HbaseConfigs.hbaseNamespaceKey -> "default",
      HbaseConfigs.hbaseTableKey -> s"""$tableName""",
      HbaseConfigs.hbaseRowKey -> "id",
      HbaseConfigs.hbaseColumnMappingKey -> "personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary")
    val dataSetName = "HBase.Local.default." + tableName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties)
    val df = dataSet.read(dataSetName, datasetProps)
    df.show(1)
    assert(df.count() == 10)
  }

  test("Write operation - column given in input via " + HbaseConfigs.hbaseColumnMappingKey + " not present in dataframe to write") {
    val props : Map[String, String] = Map(HbaseConfigs.hbaseNamespaceKey -> "default",
      HbaseConfigs.hbaseTableKey -> s"""$tableName""",
      HbaseConfigs.hbaseRowKey -> "id",
      HbaseConfigs.hbaseColumnMappingKey -> "personal:name,personal:address,personal:age,professional:company,professional:designation,professional:manager,professional:comp")
    val dataSetName = "HBase.Local.default." + tableName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties)
    val dataFrame = mockDataInDataFrame(10)
    dataFrame.show(1)
    val exception = intercept[Exception] {
      dataSet.write(dataSetName, dataFrame, datasetProps)
    }
    assert(exception.getMessage.contains("Columns : manager,comp not found in dataframe schema") == true)
  }

  test("Read operation - select specific columns") {
    val props : Map[String, String] = Map(HbaseConfigs.hbaseNamespaceKey -> "default",
      HbaseConfigs.hbaseTableKey -> s"""$tableName""",
      HbaseConfigs.hbaseRowKey -> "id",
      HbaseConfigs.hbaseColumnMappingKey -> "personal:name,personal:address,professional:company")
    val dataSetName = "HBase.Local.default." + tableName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties)
    val df = dataSet.read(dataSetName, datasetProps)
    df.show(1)
    assert(df.columns.sameElements(Array("id", "company", "name", "address")))
  }

  test("Read operation - same column in 2 column families") {
    val props : Map[String, String] = Map(HbaseConfigs.hbaseNamespaceKey -> "default",
      HbaseConfigs.hbaseTableKey -> s"""$tableName""",
      HbaseConfigs.hbaseRowKey -> "id",
      HbaseConfigs.hbaseColumnMappingKey -> "personal:name,personal:address,professional:name",
      HbaseConfigs.hbaseColumnNamewithColumnFamilyAppended -> "true")
    val dataSetName = "HBase.Local.default." + tableName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties)
    val df = dataSet.read(dataSetName, datasetProps)
    df.show(1)
    assert(df.columns.sameElements(Array("rowkey_id", "professional_name", "personal_name", "personal_address")))
  }
}
