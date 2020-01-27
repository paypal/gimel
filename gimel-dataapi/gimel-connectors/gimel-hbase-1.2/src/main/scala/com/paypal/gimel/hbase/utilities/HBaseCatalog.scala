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

import org.apache.spark.sql.types.StructType

import com.paypal.gimel.logger.Logger

/**
  * This is a Template Implementation to Create a Catalog for HBASE Spark Connector
  * Take a variety of parameters for mapping a DataFrame to its HBASE equivalent
  * 1. NameSpace
  * 2. TableName
  * 3. Keys
  * 4. Column Families with a list of columns to put in each column family
  * 5. TableCoder
  */


object HBaseCatalog {
  val logger = Logger()

  val holderNameSpace = "<NAMESPACE>"
  val holderTableName = "<TABLENAME>"
  val holderTableCoder = "<TABLECODER>"
  val holderKey = "<KEY>"
  val holderKeysAsCols = "<KEYSASCOLS>"
  val holderColumns = "<COLUMNSWITHOUTKEYS>"
  val catalogTemplate: String =
    s"""|{"table":{"namespace":"$holderNameSpace", "name":"$holderTableName", "tableCoder":"$holderTableCoder"},
        |"rowkey":"$holderKey",
        |"columns":{
        |$holderKeysAsCols,
        |$holderColumns
        |}
        |}
    """.stripMargin

  /**
    * This function creates fields as String for Catalog with column Family appending with Column Name
    *
    * @param fields       Hbase Table namespace
    * @param columnFamily Hbase table name
    */
  def fieldsAsStringForCataLogAppendColumnFamily(fields: Array[String], columnFamily: String = "rowkey"): String = {

    var lengthString = ""
    fields.map {
      eachKey =>
        val hbaseCol = if (columnFamily == "rowkey") {
          lengthString = """, "length":"50""""
          eachKey
        } else eachKey
        s""""$columnFamily""" + s"""_$eachKey":{"cf":"$columnFamily", "col":"$hbaseCol", "type":"string"$lengthString}"""
    }.mkString("", ",\n", "")
  }


  /**
    * This function creates fields as String for Catalog
    *
    * @param fields       Hbase Table namespace
    * @param columnFamily Hbase table name
    */
  def fieldsAsStringForCataLog(fields: Array[String], columnFamily: String = "rowkey"): String = {

    var lengthString = ""
    fields.map {
      eachKey =>
        val hbaseCol = if (columnFamily == "rowkey") {
          lengthString = """, "length":"50""""
          eachKey
        } else eachKey
        s""""$eachKey":{"cf":"$columnFamily", "col":"$hbaseCol", "type":"string"$lengthString}"""
    }.mkString("", ",\n", "")
  }

  /**
    * This function creates a catalog for hbase table with single column family
    *
    * @param nameSpace Hbase Table namespace
    * @param tableName Hbase table name
    * @param dfSchema  Array of columns in dataframe
    * @param keys      Array of row key columns
    * @param  columnFamily
    * @param tableCoder
    * @return String
    */
  def apply(nameSpace: String, tableName: String, dfSchema: Array[String], keys: Array[String], columnFamily: String, tableCoder: String = "PrimitiveType"): String = {
    val key = keys.mkString(":")
    val keysAsCols = fieldsAsStringForCataLog(keys)
    val columns = dfSchema.diff(keys)
    val colsAsCols = fieldsAsStringForCataLog(columns, columnFamily)
    val catalogString = catalogTemplate.
      replaceAllLiterally(holderNameSpace, nameSpace)
      .replaceAllLiterally(holderTableName, tableName)
      .replaceAllLiterally(holderTableCoder, tableCoder)
      .replaceAllLiterally(holderKey, key)
      .replaceAllLiterally(holderColumns, colsAsCols)
      .replaceAllLiterally(holderKeysAsCols, keysAsCols)
    catalogString
  }

  /**
    * This function creates a catalog for hbase table with multiple column family
    *
    * @param nameSpace Hbase Table namespace
    * @param tableName Hbase table name
    * @param cfColsMap Map[Column Family -> Array[Column Names ] ]
    * @param keys      Array of row key columns
    * @param tableCoder
    * @return String
    */

  def apply(nameSpace: String, tableName: String, cfColsMap: Map[String, Array[String]], keys: Array[String], tableCoder: String, readWithColumnFamily: Boolean): String = {
    val key = keys.mkString(":")
    val keysAsCols = if (readWithColumnFamily) {
      fieldsAsStringForCataLogAppendColumnFamily(keys)
    } else {
      fieldsAsStringForCataLog(keys)
    }
    val colsAsCols = if (readWithColumnFamily) {
      cfColsMap.map { x => fieldsAsStringForCataLogAppendColumnFamily(x._2.diff(keys), x._1) }.mkString("", ",\n", "")
    }
    else {
      cfColsMap.map { x => fieldsAsStringForCataLog(x._2.diff(keys), x._1) }.mkString("", ",\n", "")
    }
    val catalogString = catalogTemplate.
      replaceAllLiterally(holderNameSpace, nameSpace)
      .replaceAllLiterally(holderTableName, tableName)
      .replaceAllLiterally(holderTableCoder, tableCoder)
      .replaceAllLiterally(holderKey, key)
      .replaceAllLiterally(holderColumns, colsAsCols)
      .replaceAllLiterally(holderKeysAsCols, keysAsCols)
    logger.info(catalogString)
    logger.info("catalog is --> " + catalogString)
    catalogString
  }

  /**
    * This function creates a catalog for hbase table with single column family from a dataframe schema
    *
    * @param nameSpace Hbase Table namespace
    * @param tableName Hbase table name
    * @param dfSchema  Dataframe Schema
    * @param keys      Array of row key columns
    * @param  columnFamily
    * @param tableCoder
    * @return String
    */
  def apply(nameSpace: String, tableName: String, dfSchema: StructType, keys: Array[String], columnFamily: String, tableCoder: String): String = {
    val key = keys.mkString(":")
    val keysAsCols = fieldsAsStringForCataLog(keys)
    val columns = dfSchema.fieldNames.diff(keys)
    val colsAsCols = fieldsAsStringForCataLog(columns, columnFamily)
    val catalogString = catalogTemplate.
      replaceAllLiterally(holderNameSpace, nameSpace)
      .replaceAllLiterally(holderTableName, tableName)
      .replaceAllLiterally(holderTableCoder, tableCoder)
      .replaceAllLiterally(holderKey, key)
      .replaceAllLiterally(holderColumns, colsAsCols)
      .replaceAllLiterally(holderKeysAsCols, keysAsCols)
    logger.info(catalogString)
    logger.info("catalog is --> " + catalogString)
    catalogString
  }
}
