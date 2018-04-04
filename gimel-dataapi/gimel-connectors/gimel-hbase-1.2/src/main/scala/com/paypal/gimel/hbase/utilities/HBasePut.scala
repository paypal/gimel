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

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.logger.Logger

object HBasePut {

  def apply(sparkSession: SparkSession): HBasePut = new HBasePut(sparkSession)

}

class HBasePut(sparkSession: SparkSession) {
  val logger = Logger()
  val thisUser: String = sys.env(GimelConstants.USER)

  /**
    * This function performs put(insert/update) operation on each row of dataframe
    *
    * @param dataset   Name of the PCatalog Data Set
    * @param dataFrame The Dataframe to write into Target
    * @param dataSetProps
    *                  props is the way to set various additional parameters for read and write operations in DataSet class
    *                  Example Usecase : Hbase put
    *                  val props = Map("operation" -> "put")
    *                  val recsDF = dataSet.write("pcatalog.test123", df, options);
    * @return DataFrame
    */

  def put(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    try {

      // This is the DataSet Properties
      val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
      val schema: Array[String] = datasetProps.fields.map(_.fieldName)
      val tableProperties = datasetProps.props
      val tableColumnMapping = tableProperties(HbaseConfigs.hbaseColumnMappingKey)
      val rowkeyPosition = tableColumnMapping.split(",").indexOf(":key")
      val hbaseRowKeys = dataSetProps.getOrElse(HbaseConfigs.hbaseRowKey, Array(schema(rowkeyPosition))).asInstanceOf[Array[String]]
      val hbaseTable = dataSetProps.getOrElse(HbaseConfigs.hbaseTableKey, tableProperties.getOrElse(HbaseConfigs.hbaseTableKey, "")).asInstanceOf[String]
      // Setting (Column family -> Array[Columns]) mapping
      val columnFamilyFromTable: Map[String, String] = if (tableColumnMapping.split(",").length > 1 && !tableColumnMapping.split(",")(1).contains(":key")) {
        tableColumnMapping.replaceAll(":key,", "").split(",").map(x => (x.split(":")(1), x.split(":")(0))).toMap
      } else null

      val columnSet = dataFrame.columns
      putRows(hbaseTable, dataFrame, hbaseRowKeys.mkString(":"), columnSet, columnFamilyFromTable)
      dataFrame
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to put data into HBase table.")
        throw ex
    }
  }

  /**
    *
    * @param hbaseTable   Hbase Table Name
    * @param dataFrame    The Dataframe to put into Target
    * @param rowKeyColumn Name of the row Key column in hive table
    * @param columns      Array of Columns to be put
    * @param cfColsMap    Map of (Column -> Column Family)
    */
  def putRows(hbaseTable: String, dataFrame: DataFrame, rowKeyColumn: String, columns: Array[String], cfColsMap: Map[String, String]) {
    try {
      // Configure And Connect
      val conf = HBaseConfiguration.create()
      val cnxn = ConnectionFactory.createConnection(conf)
      // Create Connection to HBase table
      val tbl = cnxn.getTable(TableName.valueOf(hbaseTable))
      val rows = dataFrame.rdd.map { row =>
        (row.getAs(rowKeyColumn).toString,
          columns.map(eachCol => (cfColsMap.getOrElse(eachCol, ""), eachCol, row.getAs(eachCol).asInstanceOf[String]))
          )
      }.collect()
      // Performing put operation on each row of dataframe
      rows.foreach { row =>
        val putRow: Put = new Put(Bytes.toBytes(row._1.asInstanceOf[String]))
        row._2.foreach(x => if (x._2 != rowKeyColumn) putRow.addColumn(Bytes.toBytes(x._1), Bytes.toBytes(x._2), Bytes.toBytes(x._3)))
        tbl.put(putRow)
      }
      tbl.close()
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }
}
