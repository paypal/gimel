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

import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan}
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._

import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.logger.Logger

object HBaseScanner {

  def apply(): HBaseScanner = new HBaseScanner()

}

class HBaseScanner() {

  val logger = Logger(this.getClass)

  /**
    * Returns schema of hbase table
    *
    * @param connection
    * @param namespace
    * @param tableName
    * @param maxResults : Number of maximum records to be scanned
    * @return Map of [Column Family -> Array[Columns] ]
    */
  def getSchema(connection: Connection, namespace: String, tableName: String, rowKey: String, maxResults: Int): Map[String, Array[String]] = {
    val table: TableName = TableName.valueOf(namespace + ":" + tableName)
    val tbl = connection.getTable(table)
    // INITIATE SCANNER
    val scan = new Scan()

    // Setting the Page Filter to retrieve pageSize records from each region server
    val pageSize = getPageSize(connection, table, maxResults)
    logger.info("Setting the pageSize = " + pageSize)
    val filter = new PageFilter(maxResults)
    scan.setFilter(filter)

    var count = 0
    // Iterate through all the records retrieved from HBase and get column family and column names
    GenericUtils.withResources(tbl.getScanner(scan)) { scanner =>
      val res = scanner.iterator().asScala.flatMap { result =>
        count = count + 1
        val cells = result.listCells().iterator().asScala
        cells.map(cell => (Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)))).toList
      }.toList.distinct.groupBy(_._1).map(x => (x._1, x._2.map(p => p._2).toArray))
      logger.info(s"Records Count for ${tableName} : " + count)
      val rowKeyMap = Map("rowKey" -> Array(rowKey))
      rowKeyMap ++ res
    }
  }

  /**
    * Returns schema of hbase table with specified maximum number of columns and result size
    *
    * @param connection
    * @param namespace
    * @param tableName
    * @param maxResults : Number of maximum records to be scanned
    * @param maxColumns : Number of maximum columns to be scanned
    * @param maxResultSize : Maximum result size in bytes
    * @return Map of [Column Family -> Array[Columns] ]
    */
  def getSchema(connection: Connection, namespace: String, tableName: String, maxResults: Int, maxColumns: Int, maxResultSize : Long): Map[String, Array[String]] = {
    val table: TableName = TableName.valueOf(namespace + ":" + tableName)
    val tbl = connection.getTable(table)
    // INITIATE SCANNER
    val scan = new Scan()

    // Setting the Page Filter to retrieve pageSize records from each region server
    val pageSize = getPageSize(connection, table, maxResults)
    logger.info("Setting the pageSize = " + pageSize)
    val fil = new PageFilter(maxResults)
    scan.setFilter(fil)
    // Setting the maximum result size in bytes
    scan.setMaxResultSize(maxResultSize)

    var count = 0
    var columnsCount = 0
    // Iterate through all the records retrieved from HBase and get column family and column names
    GenericUtils.withResources(tbl.getScanner(scan)) { scanner =>
      val res = scanner.iterator().asScala.takeWhile(_ => columnsCount < maxColumns).flatMap { result =>
        count = count + 1
        val cells = result.listCells()
        columnsCount = cells.size()
        val cellsItr = cells.iterator().asScala
        // Escape each column family and column in case of any special characters
        cellsItr.map(cell => (StringEscapeUtils.escapeJava(Bytes.toString(CellUtil.cloneFamily(cell))),
          StringEscapeUtils.escapeJava(Bytes.toString(CellUtil.cloneQualifier(cell))))).toList
      }.toList.distinct.groupBy(_._1).map(x => (x._1, x._2.map(p => p._2).toArray))
      logger.info(s"Records Count for ${tableName} : " + count)
      res
    }
  }

  /**
    * Returns schema of hbase table with specified maximum number of columns
    *
    * @param connection
    * @param namespace
    * @param tableName
    * @param maxResults : Number of maximum records to be scanned
    * @param maxColumns : Number of maximum columns to be scanned
    * @return Map of [Column Family -> Array[Columns] ]
    */
  def getSchema(connection: Connection, namespace: String, tableName: String, maxResults: Int, maxColumns: Int): Map[String, Array[String]] = {
    val table: TableName = TableName.valueOf(namespace + ":" + tableName)
    val tbl = connection.getTable(table)
    // INITIATE SCANNER
    val scan = new Scan()

    // Setting the Page Filter to retrieve pageSize records from each region server
    val pageSize = getPageSize(connection, table, maxResults)
    logger.info("Setting the pageSize = " + pageSize)
    val filter = new PageFilter(pageSize)
    scan.setFilter(filter)

    var count = 0
    var columnsCount = 0
    // Iterate through all the records retrieved from HBase and get column family and column names
    GenericUtils.withResources(tbl.getScanner(scan)) { scanner =>
      val res = scanner.iterator().asScala.takeWhile(_ => columnsCount < maxColumns).flatMap { result =>
        count = count + 1
        val cells = result.listCells()
        columnsCount = cells.size()
        val cellsItr = cells.iterator().asScala
        // Escape each column family and column in case of any special characters
        cellsItr.map(cell => (StringEscapeUtils.escapeJava(Bytes.toString(CellUtil.cloneFamily(cell))),
          StringEscapeUtils.escapeJava(Bytes.toString(CellUtil.cloneQualifier(cell))))).toList
      }.toList.distinct.groupBy(_._1).map(x => (x._1, x._2.map(p => p._2).toArray))
      logger.info(s"Records Count for ${tableName} : " + count)
      res
    }
  }

  /**
    * Returns page size based on the number of regions and maxResults size
    *
    * @param connection
    * @param table
    * @param maxResults : Number of maximum records to be scanned
    * @return Page Size
    */
  def getPageSize(connection: Connection, table: TableName, maxResults: Int): Int = {
    // Getting total region servers to decide the PageFilter size
    val regionLocator = connection.getRegionLocator(table)
    val numRegionServers = regionLocator.getAllRegionLocations().asScala.map(eachRegion => eachRegion.getHostname()).distinct.size
    if (numRegionServers == 0) {
      0
    } else {
      Math.max(maxResults / numRegionServers, 1)
    }
  }

  /**
    * Returns schema of hbase table by creating a connection
    *
    * @param namespace : Name of hbase name space
    * @param tableName : Name of hbase table
    * @param maxResults : Number of maximum records to be scanned
    * @param maxColumns : Number of maximum columns to be scanned
    * @return Map of [Column Family -> Array[Columns] ]
    */
  def getSchema(namespace: String, tableName: String, maxResults: Int, maxColumns: Int): Map[String, Array[String]] = {
    val conf = HBaseConfiguration.create()
    GenericUtils.withResources(ConnectionFactory.createConnection(conf)) {
      connection =>
        getSchema(connection, namespace, tableName, maxResults, maxColumns)
    }
  }

  /**
    * Returns schema of hbase table by creating a connection with specified maximum number of columns
    *
    * @param namespace
    * @param tableName
    * @param maxResults : Number of maximum records to be scanned
    * @param maxColumns : Number of maximum columns to be scanned
    * @param maxResultSize : Maximum result size in bytes
    * @return Map of [Column Family -> Array[Columns] ]
    */
  def getSchema(namespace: String, tableName: String, maxResults: Int, maxColumns: Int, maxResultSize : Long): Map[String, Array[String]] = {
    val conf = HBaseConfiguration.create()
    GenericUtils.withResources(ConnectionFactory.createConnection(conf)) {
      connection =>
        getSchema(connection, namespace, tableName, maxResults, maxColumns, maxResultSize)
    }
  }
}
