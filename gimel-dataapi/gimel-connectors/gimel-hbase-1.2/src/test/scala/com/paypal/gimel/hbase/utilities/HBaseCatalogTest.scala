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

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{FunSpec, Matchers}

class HBaseCatalogTest extends FunSpec with Matchers {

  describe("fieldsAsStringForCataLogAppendColumnFamily") {
    it ("should create json of fields with type as string for Catalog with column Family appended with Column Name") {
      HBaseCatalog.fieldsAsStringForCataLogAppendColumnFamily(columnsList, "cf1") should be (
        s""""cf1_c1":{"cf":"cf1", "col":"c1", "type":"string"},
           |"cf1_c2":{"cf":"cf1", "col":"c2", "type":"string"},
           |"cf1_c3":{"cf":"cf1", "col":"c3", "type":"string"}""".stripMargin)

      HBaseCatalog.fieldsAsStringForCataLogAppendColumnFamily(keyList, "rowkey") should be (
        s""""rowkey_key1":{"cf":"rowkey", "col":"key1", "type":"string", "length":"50"},
           |"rowkey_key2":{"cf":"rowkey", "col":"key2", "type":"string", "length":"50"}""".stripMargin)
    }
  }

  describe("fieldsAsStringForCataLog") {
    it ("should create json of fields with type as string for Catalog") {
      HBaseCatalog.fieldsAsStringForCataLog(columnsList, "cf1") should be (
        s""""c1":{"cf":"cf1", "col":"c1", "type":"string"},
           |"c2":{"cf":"cf1", "col":"c2", "type":"string"},
           |"c3":{"cf":"cf1", "col":"c3", "type":"string"}""".stripMargin)

      HBaseCatalog.fieldsAsStringForCataLog(keyList, "rowkey") should be (
        s""""key1":{"cf":"rowkey", "col":"key1", "type":"string", "length":"50"},
           |"key2":{"cf":"rowkey", "col":"key2", "type":"string", "length":"50"}""".stripMargin)
    }
  }

  describe("HBaseCatalog") {
    it ("should create a catalog string with one column family and df columns array for shc connector") {
      HBaseCatalog("namespace", "tablename", columnsList, keyList, "cf1") should be
        s"""{"table":{"namespace":"namespace", "name":"tablename", "tableCoder":"PrimitiveType"},
            |"rowkey":"key1:key2",
            |"columns":{
            |"key1":{"cf":"rowkey", "col":"key1", "type":"string", "length":"50"},
            |"key2":{"cf":"rowkey", "col":"key2", "type":"string", "length":"50"},
            |"c1":{"cf":"cf1", "col":"c1", "type":"string"},
            |"c2":{"cf":"cf1", "col":"c2", "type":"string"},
            |"c3":{"cf":"cf1", "col":"c3", "type":"string"}
            |}
            |}
            |""".stripMargin
    }

    it ("should create a catalog string with one column family and df schema for shc connector") {
      HBaseCatalog("namespace", "tablename", schema, keyList, "cf1", "PrimitiveType") should be
        s"""{"table":{"namespace":"namespace", "name":"tablename", "tableCoder":"PrimitiveType"},
            |"rowkey":"key1:key2",
            |"columns":{
            |"key1":{"cf":"rowkey", "col":"key1", "type":"string", "length":"50"},
            |"key2":{"cf":"rowkey", "col":"key2", "type":"string", "length":"50"},
            |"num":{"cf":"cf1", "col":"num", "type":"string"},
            |"letter":{"cf":"cf1", "col":"letter", "type":"string"}
            |}
            |}
            |""".stripMargin
    }

    it ("should create a catalog string with multiple column families for shc connector") {
      // With column family appended
      HBaseCatalog("namespace", "tablename", columnFamilyToColumnMapping, keyList, "PrimitiveType", true) should be
        s"""{"table":{"namespace":"namespace", "name":"tablename", "tableCoder":"PrimitiveType"},
            |"rowkey":"key1:key2",
            |"columns":{
            |"rowkey_key1":{"cf":"rowkey", "col":"key1", "type":"string", "length":"50"},
            |"rowkey_key2":{"cf":"rowkey", "col":"key2", "type":"string", "length":"50"},
            |"cf1_c11":{"cf":"cf1", "col":"c11", "type":"string"},
            |"cf1_c12":{"cf":"cf1", "col":"c12", "type":"string"},
            |"cf2_c21":{"cf":"cf2", "col":"c21", "type":"string"},
            |"cf2_c22":{"cf":"cf2", "col":"c22", "type":"string"}
            |}
            |}
            |""".stripMargin

      // Without column family appended
      HBaseCatalog("namespace", "tablename", columnFamilyToColumnMapping, keyList, "PrimitiveType", false) should be
        s"""{"table":{"namespace":"namespace", "name":"tablename", "tableCoder":"PrimitiveType"},
            |"rowkey":"key1:key2",
            |"columns":{
            |"key1":{"cf":"rowkey", "col":"key1", "type":"string", "length":"50"},
            |"key2":{"cf":"rowkey", "col":"key2", "type":"string", "length":"50"},
            |"c11":{"cf":"cf1", "col":"c11", "type":"string"},
            |"c12":{"cf":"cf1", "col":"c12", "type":"string"},
            |"c21":{"cf":"cf2", "col":"c21", "type":"string"},
            |"c22":{"cf":"cf2", "col":"c22", "type":"string"}
            |}
            |}
            |""".stripMargin

    }
  }

  val schema: StructType = StructType(
    List(
      StructField("num", IntegerType, true),
      StructField("letter", StringType, true)
    )
  )

  val columnFamilyToColumnMapping = Map("cf1" -> Array("c11", "c12"),
    "cf2" -> Array("c21", "c22"))

  val keyList = Array("key1", "key2")

  val columnsList = Array("c1", "c2", "c3")
}
