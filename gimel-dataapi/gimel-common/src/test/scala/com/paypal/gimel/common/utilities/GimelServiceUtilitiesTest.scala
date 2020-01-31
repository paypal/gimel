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

package com.paypal.gimel.common.utilities

import org.scalatest.FunSuite

import com.paypal.gimel.common.gimelservices.GimelServiceUtilities

class GimelServiceUtilitiesTest extends FunSuite {

  val gimelServices = new GimelServiceUtilities()

  test("getObjectPropertiesForSystem") {
    println("1. storageTypeName=HIVE, dataset=Hive.Cluster.default.flights")
    assert(gimelServices.getObjectPropertiesForSystem("HIVE", "Hive.Cluster.default.flights")
      .sameElements(scala.collection.mutable.Map[String, String]("gimel.hive.db.name" -> "default",
        "gimel.hive.table.name" -> "flights")))

    println("2. storageTypeName=TERADATA, dataset=Teradata.Cluster.flights_db.flights")
    assert(gimelServices.getObjectPropertiesForSystem("TERADATA", "Teradata.Cluster.flights_db.flights")
      .sameElements(scala.collection.mutable.Map[String, String]("gimel.jdbc.input.table.name" -> "flights_db.flights")))

    println("3. storageTypeName=MYSQL, dataset=MySql.gimelmysql.gimeldb.gimeltable")
    assert(gimelServices.getObjectPropertiesForSystem("MYSQL", "MySql.gimelmysql.gimeldb.gimeltable")
      .sameElements(scala.collection.mutable.Map[String, String]("gimel.jdbc.input.table.name" -> "gimeldb.gimeltable")))

    println("4. storageTypeName=ELASTIC, dataset=Elastic.Gimel_Dev.default.gimel_tau_flights")
    assert(gimelServices.getObjectPropertiesForSystem("ELASTIC", "Elastic.Gimel_Dev.default.gimel_tau_flights")
      .sameElements(scala.collection.mutable.Map[String, String]("es.resource" -> "default/gimel_tau_flights",
        "es.index.auto.create" -> "true")))

    println("5. storageTypeName=HBASE, dataset=Hbase.Horton.default.test_table")
    assert(gimelServices.getObjectPropertiesForSystem("HBASE", "Hbase.Horton.default.test_table")
      .sameElements(scala.collection.mutable.Map[String, String]("gimel.hbase.namespace.name" -> "default",
        "gimel.hbase.table.name" -> "test_table")))

    println("6. storageTypeName=S3, dataset=S3.Dev.default.test_object")
    val exception = intercept[Exception] {
      gimelServices.getObjectPropertiesForSystem("S3", "S3.Dev.default.test_object")
    }
    assert(exception.getMessage.contains(s"""does not exist. Please check if the dataset name is correct."""))
  }
}
