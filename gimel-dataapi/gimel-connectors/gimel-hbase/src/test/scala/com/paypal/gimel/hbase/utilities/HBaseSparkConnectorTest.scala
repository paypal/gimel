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

import org.scalatest.{BeforeAndAfterAll, Matchers}

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.hbase.conf.HbaseConfigs

class HBaseSparkConnectorTest extends HBaseLocalClient with Matchers with BeforeAndAfterAll {

  test("write operation") {
    val props : Map[String, String] = Map(HbaseConfigs.hbaseNamespaceKey -> "default",
      HbaseConfigs.hbaseTableKey -> s"""$tableName""",
      HbaseConfigs.hbaseRowKey -> "id",
      HbaseConfigs.hbaseColumnMappingKey -> "personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary")
    val dataSetName = "HBase.Local.default." + tableName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties)
    val dataFrame = mockDataInDataFrame(1000)
    dataFrame.show(1)
    val df = HBaseSparkConnector(sparkSession).write(dataSetName, dataFrame, datasetProps)
    assert(df.count() == 1000)
  }

  test("read operation") {
    val props : Map[String, String] = Map(HbaseConfigs.hbaseNamespaceKey -> "default",
      HbaseConfigs.hbaseTableKey -> s"""$tableName""",
      HbaseConfigs.hbaseRowKey -> "id",
      HbaseConfigs.hbaseColumnMappingKey -> "personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary")
    val dataSetName = "HBase.Local.default." + tableName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties)
    val df = HBaseSparkConnector(sparkSession).read(dataSetName, datasetProps)
    df.show(1)
    assert(df.count() == 1000)
  }

  test("read operation with page size") {
    val props : Map[String, String] = Map(HbaseConfigs.hbaseNamespaceKey -> "default",
      HbaseConfigs.hbaseTableKey -> s"""$tableName""",
      HbaseConfigs.hbaseRowKey -> "id",
      HbaseConfigs.hbaseColumnMappingKey -> "personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary")
    sparkSession.conf.set(GimelConstants.HBASE_PAGE_SIZE, 20)
    val dataSetName = "HBase.Local.default." + tableName
    val dataSetProperties = DataSetProperties(dataSetName, null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties"->dataSetProperties)
    val df = HBaseSparkConnector(sparkSession).read(dataSetName, datasetProps)
    df.show(20)
    val metricInsertQuery = metrics(metrics.length - 1)
    val qe = metricInsertQuery._2
    println(qe.executedPlan.children(0).children(0).children(0).metrics)
    val kafkaReadOutputRows = qe.executedPlan.children(0).children(0).children(0).metrics("numOutputRows").value
    assert(kafkaReadOutputRows == 20)
    sparkSession.conf.unset(GimelConstants.HBASE_PAGE_SIZE)
  }
}
