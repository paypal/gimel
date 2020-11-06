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

import scala.collection.JavaConverters._
import scala.collection.immutable.{Iterable, Map}

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import spray.json._
import spray.json.DefaultJsonProtocol._

import com.paypal.gimel.hbase.conf.{HbaseClientConfiguration, HbaseConfigs}
import com.paypal.gimel.logger.Logger

object HBaseLookUp {

  def apply(sparkSession: SparkSession): HBaseLookUp = new HBaseLookUp(sparkSession)

}

class HBaseLookUp(sparkSession: SparkSession) {

  val logger = Logger()

  /**
    * This function reads all or given columns in column family for a rowKey specified by user
    *
    * @param dataset Name
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : Hbase lookup for rowKey=r1 and columns c1, c12 of column family cf1 and c2 of cf2
    *                val options: Map[String, Any] = Map("operation"->"get","filter"->"rowKey=r1:toGet=cf1-c1,c12|cf2-c2")
    *                val recsDF = dataSet.read("udc.test123", options);
    * @return DataFrame
    */
  def get(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {
    try {

      // Hbase configuration
      val conf = new HbaseClientConfiguration(dataSetProps)
      var options = Map.empty[String, String]
      try {
        if (conf.getOption.isEmpty) {
          throw new IllegalArgumentException(
            s"""
               | HBase get filter condition not found. Please set the property ${HbaseConfigs.hbaseFilter}.
               | Example: rowKey=1:toGet=personal
               | where personal is the column family name
               |""".stripMargin)
        }
        options = conf.getOption.split(":").map { x => x.split("=")(0) -> x.split("=")(1) }.toMap
        if (!options.contains("rowKey")) {
          throw new IllegalArgumentException(
            s"""
               | rowKey not present in the filter condition. Please check the property ${HbaseConfigs.hbaseFilter}.
               | Examples: rowKey=1:toGet=personal
               | where personal is the column family name
               |""".stripMargin)
        }
      } catch {
        case ex: Throwable =>
          logger.error(
            s"""
               | Unable to parse the filter condition. Please check the property ${HbaseConfigs.hbaseFilter}
               | Example: rowKey=1:toGet=personal
               | where personal is the column family name
               |""".stripMargin)
          ex.printStackTrace()
          throw ex
      }

      val rowKey = options("rowKey")

      val dataFromHBASE: Map[String, String] = if (!options.contains("toGet")) {
        getColumnsInRowKey(conf.hbaseNameSpace + ":" + conf.hbaseTableName, rowKey)
      } else {
        val cfsAndCols = options("toGet")
        // (Column family to Array[Columns]) mapping specified by user in toGet
        val cfsSets: Map[String, Array[String]] = cfsAndCols.split('|').map { x =>
          if (x.split("-").length > 1) x.split('-')(0) -> x.split('-')(1).split(',') else x.split('-')(0) -> null
        }.toMap
        getColumnsInRowKey(conf.hbaseNameSpace + ":" + conf.hbaseTableName, rowKey, cfsSets)
      }
      val hbaseDataJSON = dataFromHBASE.toJson.compactPrint
      val hbaseDf = jsonStringToDF(sparkSession, hbaseDataJSON)
      hbaseDf
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to get data from HBase table.")
        throw ex
    }
  }

  /**
    * Returns all/specified columns in column family for a rowKey specified by user
    *
    * @param hbaseTable Name of the Data Set
    * @param rowKey     row Key for the lookup
    * @param cfsSets    User Specified column family and columns
    * @return Map[Column -> Column Value ]
    */
  def getColumnsInRowKey(hbaseTable: String, rowKey: String, cfsSets: Map[String, Array[String]]): Map[String, String] = {
    val k: Iterable[Map[String, String]] = cfsSets.map { x =>
      val cf1 = x._1
      val cols = x._2
      val hbaseData = getColumnsInFamily(hbaseTable, rowKey, cf1, cols)
      hbaseData
    }
    val foldedMap: Map[String, String] = k.tail.foldLeft(k.head)((x, y) => x ++ y)
    foldedMap
  }

  /**
    * Converts a String to DataFrame
    *
    * @param sqlCntxt SQLContext
    * @param string   Input String (must be JSON Format)
    */
  def jsonStringToDF(sqlCntxt: SQLContext, string: String): DataFrame = {
    val rdd = sqlCntxt.sparkContext.parallelize(Seq(string))
    sqlCntxt.read.json(rdd)
  }

  /**
    * Converts a String to DataFrame
    *
    * @param sparkSession : SparkSession
    * @param string       Input String (must be JSON Format)
    */
  def jsonStringToDF(sparkSession: SparkSession, string: String): DataFrame = {
    val rdd = sparkSession.sparkContext.parallelize(Seq(string))
    sparkSession.read.json(rdd)
  }

  /**
    * Returns Column Value for each column in a column family
    *
    * @param hbaseTable   HBASE Table Name
    * @param rowKey       Row Key
    * @param columnFamily Column Family Name
    * @param columns      Array of Column Names
    * @return Map[Column -> Column Value ]
    */
  def getColumnsInFamily(hbaseTable: String, rowKey: String, columnFamily: String, columns: Array[String]): Map[String, String] = {
    try {
      val hbaseColumnFamily: Array[Byte] = Bytes.toBytes(columnFamily)
      val hTable = TableName.valueOf(hbaseTable)
      val rowKeyBytes = Bytes.toBytes(rowKey)
      val getRowKey: Get = new Get(rowKeyBytes)
      // Configure And Connect
      val conf = HBaseConfiguration.create()
      val cnxn = ConnectionFactory.createConnection(conf)
      // Get Operation
      val tbl = cnxn.getTable(hTable)
      val k: Result = tbl.get(getRowKey)

      // Get Column values of each column as Map of [Column Name -> Column Value]
      val allColumns: Map[String, String] = columns match {
        // If user specifies only column family, get all the columns in that column family otherwise get specified columns
        case null =>
          k.getFamilyMap(Bytes.toBytes(columnFamily)).asScala.map(x => (Bytes.toString(x._1), Bytes.toString(x._2))).toMap
        case _ =>
          // Columns Bytes
          val hbaseColumns = columns.map(Bytes.toBytes)
          // Mapping Cf with Columns into single collection
          val cfAndColumns: Array[(Array[Byte], Array[Byte])] = hbaseColumns.map((hbaseColumnFamily, _))
          // Return requested Columns and their values in a Map
          val allColumns = cfAndColumns.map { x =>
            Bytes.toString(x._2) -> Bytes.toString(k.getValue(x._1, x._2))
          }.toMap
          allColumns
      }
      allColumns
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }

  }

  /**
    * Returns all columns in all column families for a rowKey specified by user
    *
    * @param hbaseTable Name of the Data Set
    * @param rowKey     row Key for the lookup
    * @return Map[Column -> Column Value ]
    */
  def getColumnsInRowKey(hbaseTable: String, rowKey: String): Map[String, String] = {
    try {
      val hTable = TableName.valueOf(hbaseTable)
      val rowKeyBytes = Bytes.toBytes(rowKey)
      val getRowKey: Get = new Get(rowKeyBytes)
      // Configure And Connect
      val conf = HBaseConfiguration.create()
      val cnxn = ConnectionFactory.createConnection(conf)
      // Get Operation
      val tbl = cnxn.getTable(hTable)
      val k: Result = tbl.get(getRowKey)
      val columnsVals = k.rawCells().map(cell => (Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)))).toMap
      tbl.close()
      columnsVals
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

}
