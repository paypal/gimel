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

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan}
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.util.Bytes

import com.paypal.gimel.logger.Logger

object HBaseScanner {

  def apply(): HBaseScanner = new HBaseScanner()

}

class HBaseScanner() {

  val logger = Logger(this.getClass)

  /**
    * Returns schema of hbase table
    *
    * @param tableName : Name of hbase table
    * @return Map of [Column Family -> Array[Columns] ]
    */
  def getSchema(connection: Connection, namespace: String, tableName: String, rowKey: String, maxResults: Int): Map[String, Array[String]] = {
    val table: TableName = TableName.valueOf(namespace + ":" + tableName)
    val tbl = connection.getTable(table)
    // INITIATE SCANNER
    val scan = new Scan()
    val fil = new PageFilter(maxResults)
    scan.setFilter(fil)
    val scanner = tbl.getScanner(scan)

    val rs = tbl.getScanner(scan)
    var count = 0
    try {
      val res = scanner.iterator().asScala.flatMap { result =>
        count = count + 1
        val cells = result.listCells().iterator().asScala
        cells.map(cell => (Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)))).toList
      }.toList.distinct.groupBy(_._1).map(x => (x._1, x._2.map(p => p._2).toArray))
      logger.info(s"Records Count for ${tableName} : " + count)
      val rowKeyMap = Map("rowKey" -> Array(rowKey))
      rowKeyMap ++ res
    } finally {
      rs.close()
    }
  }

  /**
    * Returns schema of hbase table
    *
    * @param tableName : Name of hbase table
    * @return Map of [Column Family -> Array[Columns] ]
    */
  def getSchema(namespace: String, tableName: String, rowKey: String, maxResults: Int): Map[String, Array[String]] = {
    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    try {
      getSchema(connection, namespace, tableName, rowKey, maxResults)
    } finally {
      connection.close()
    }
  }
}
