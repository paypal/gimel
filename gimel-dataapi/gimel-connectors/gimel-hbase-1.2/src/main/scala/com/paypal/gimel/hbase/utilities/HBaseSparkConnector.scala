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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.storageadmin.HBaseAdminClient
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.logger.Logger

/**
  * HBASE implementations internal to PCatalog
  */
object HBaseSparkConnector {

  def apply(sparkSession: SparkSession): HBaseSparkConnector = new HBaseSparkConnector(sparkSession)

}

class HBaseSparkConnector(sparkSession: SparkSession) {
  val logger = Logger()
  val thisUser: String = sys.env(GimelConstants.USER)

  /**
    * This function performs scan/bulkGet on hbase table
    *
    * @param dataset Name of the PCatalog Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to get 10 factor parallelism (specifically)
    *                val props = Map("coalesceFactor" -> 10)
    *                val data = Dataset(sc).read("flights", props)
    *                data.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */
  def read(dataset: String, dataSetProps: Map[String, Any] = Map.empty): DataFrame = {
    try {

      val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
      val schema: Array[String] = datasetProps.fields.map(_.fieldName)
      val tableProperties = datasetProps.props
      val tableColumnMappingInput = dataSetProps.getOrElse(HbaseConfigs.hbaseColumnMappingKey, "").asInstanceOf[String]
      val tableColumnMapping: String = if (tableColumnMappingInput.isEmpty) {
        tableProperties(HbaseConfigs.hbaseColumnMappingKey).toString
      }
      else {
        tableColumnMappingInput
      }
      val rowkeyPosition = if (!tableColumnMapping.isEmpty && tableColumnMapping.contains(":key")) {
        tableColumnMapping.split(",").indexOf(":key")
      } else {
        0
      }
      // Getting Row Key from user otherwise from DDL
      val hbaseRowKeys = dataSetProps.getOrElse(HbaseConfigs.hbaseRowKey, schema(rowkeyPosition)).asInstanceOf[String].split(",")
      logger.info("Hbase Row Keys : " + hbaseRowKeys.mkString(","))
      val hbaseTable = dataSetProps.getOrElse(HbaseConfigs.hbaseTableKey, tableProperties.getOrElse(HbaseConfigs.hbaseTableKey, "")).asInstanceOf[String]
      val hbaseNameSpace = dataSetProps.getOrElse(GimelConstants.HBASE_NAMESPACE, tableProperties.getOrElse(GimelConstants.HBASE_NAMESPACE, "default")).asInstanceOf[String]
      // Setting (Column family -> array of columns) mapping
      val columnFamilyFromTable: Map[String, Array[String]] = if (tableColumnMapping.split(",").length > 0) {
        tableColumnMapping.replaceAll(":key,", "").split(",").map(x => (x.split(":")(0), x.split(":")(1))).groupBy(_._1).map { case (k, v) => (k, v.map(_._2)) }
      } else {
        null
      }
      val hbaseSiteXMLHDFS = tableProperties.getOrElse(HbaseConfigs.hbaseSiteXMLHDFSPathKey, "NONE")
      val hbaseConfigFileLocation = HBaseAdminClient.getHbaseSiteXml(hbaseSiteXMLHDFS)
      if (columnFamilyFromTable == null) {
        logger.error("hbase.columns.mapping not found in hive table schema.")
      }
      val hbaseTableName = hbaseTable.split(":")(1)
      val catalog = HBaseCatalog(hbaseNameSpace, hbaseTableName, columnFamilyFromTable, hbaseRowKeys, "PrimitiveType")
      logger.info(s"reading with catalog --> $catalog")

      val dataframe = hbaseSiteXMLHDFS match {
        case "NONE" =>
          readWithCatalog(catalog)
        case _ =>
          readWithCatalog(catalog, hbaseConfigFileLocation)
      }
      dataframe
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to read data from HBase table.")
        throw ex
    }
  }

  /**
    * This function reads data from HBase with catalog string.
    *
    * @param catalog
    * @return
    */
  private def readWithCatalog(catalog: String): DataFrame = {
    try {
      sparkSession
        .read
        .options(Map((HBaseTableCatalog.tableCatalog, catalog)))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }
    catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * This function reads data from HBase with catalog string.
    *
    * @param catalog
    * @param hbaseConfigFileLocation The HBASE Configuration File : hbase-site.xml
    * @return DataFrame
    */
  private def readWithCatalog(catalog: String, hbaseConfigFileLocation: String): DataFrame = {
    try {
      sparkSession
        .read
        .options(Map((HBaseTableCatalog.tableCatalog, catalog), (HBaseRelation.HBASE_CONFIGFILE, hbaseConfigFileLocation)))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * This function performs bulk write into hbase table
    *
    * @param dataset   Name of the PCatalog Data Set
    * @param dataFrame The Dataframe to write into Target
    * @param dataSetProps
    *                  Example Usecase : we want only 1 executor for hbase (specifically)
    *                  val props = Map("coalesceFactor" -> 1)
    *                  Dataset(sc).write(clientDataFrame, props)
    *                  Inside write implementation :: dataFrame.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */

  def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    try {
      val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
      val schema: Array[String] = datasetProps.fields.map(_.fieldName)
      val tableProperties = datasetProps.props
      val tableColumnMappingSerDe = tableProperties(HbaseConfigs.hbaseColumnMappingKey)
      val rowkeyPosition = if (!tableColumnMappingSerDe.isEmpty) tableColumnMappingSerDe.split(",").indexOf(":key") else 0
      val hbaseRowKeys = dataSetProps.getOrElse(HbaseConfigs.hbaseRowKey, schema(rowkeyPosition)).asInstanceOf[String].split(",")
      logger.info("Hbase Row Keys : " + hbaseRowKeys.mkString(","))
      if (!(hbaseRowKeys.diff(dataFrame.columns.toSeq).isEmpty)) logger.error("Row Key column not found in input dataframe.")
      val dfColumns = dataFrame.columns.filter(x => !hbaseRowKeys.contains(x)).toSeq
      val useColumnsSpecifiedFlag = dataSetProps.getOrElse(HbaseConfigs.hbaseUseColumnsSpecifiedFlag, false).asInstanceOf[Boolean]
      // Get table column family and column mapping from input option from user
      val tableColumnMappingInput = dataSetProps.getOrElse(HbaseConfigs.hbaseColumnMappingKey, "").asInstanceOf[String]
      if (useColumnsSpecifiedFlag && tableColumnMappingInput.isEmpty) logger.error("You need to specify hbase.columns.mapping option with hbase.columns.specified.flag.")
      val tableColumnMapping = if (useColumnsSpecifiedFlag) tableColumnMappingInput else tableColumnMappingSerDe + "," + tableColumnMappingInput
      val columnsInSchema = tableColumnMapping.split(",").map(eachCol => eachCol.split(":")(1)).distinct.toSeq
      logger.info("Columns in schema : " + columnsInSchema)
      val diff = if (useColumnsSpecifiedFlag) columnsInSchema.diff(dfColumns) else dfColumns.diff(columnsInSchema)
      val columnFamilyToColumns: Map[String, Array[String]] = if (diff.isEmpty) {
        tableColumnMapping.replaceAll(":key,", "").split(",").map(x => (x.split(":")(0), x.split(":")(1))).groupBy(_._1).map { case (k, v) => (k, v.map(_._2)) }
      } else {
        logger.error("Column Families for columns : " + diff.mkString(",") + " not found in schema. You can specify the mapping in the option: " + HbaseConfigs.hbaseColumnMappingKey)
        null
      }
      val dataFrameToWrite = if (useColumnsSpecifiedFlag) dataFrame.selectExpr(columnsInSchema ++ hbaseRowKeys: _*) else dataFrame
      val hbaseTable = dataSetProps.getOrElse(HbaseConfigs.hbaseTableKey, tableProperties.getOrElse(HbaseConfigs.hbaseTableKey, "")).asInstanceOf[String]
      val hbaseNameSpace = dataSetProps.getOrElse(GimelConstants.HBASE_NAMESPACE, tableProperties.getOrElse(GimelConstants.HBASE_NAMESPACE, "default")).asInstanceOf[String]
      // Setting (Column family -> array of columns) mapping
      val hbaseSiteXMLHDFS = tableProperties.getOrElse(HbaseConfigs.hbaseSiteXMLHDFSPathKey, "NONE")
      val hbaseConfigFileLocation = HBaseAdminClient.getHbaseSiteXml(hbaseSiteXMLHDFS)
      val hbaseTableName = hbaseTable.split(":")(1)
      val catalog = HBaseCatalog(hbaseNameSpace, hbaseTableName, columnFamilyToColumns, hbaseRowKeys, "PrimitiveType")
      logger.info(s"writing with catalog --> $catalog")
      val dataframe = hbaseSiteXMLHDFS match {
        case "NONE" =>
          logger.info(s"PLAIN WRITE")
          writeWithCatalog(dataFrameToWrite, catalog)
        case _ => logger.info(s"write with ${hbaseSiteXMLHDFS}")
          writeWithCatalog(dataFrameToWrite, catalog, hbaseConfigFileLocation)
      }
      dataFrame
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"Unable to write data to HBase table.")
        throw ex
    }
  }

  /**
    * This function writes data to HBase with catalog string.
    *
    * @param dataFrame               DataFrame to write to HBase.
    * @param hbaseConfigFileLocation The HBASE Configuration File : hbase-site.xml
    * @param catalog                 Catalog string holdin schema for HBase table.
    */
  // HBaseTableCatalog.newTable Property needs to be set as a default paramaneter as SHC Connector expects this argumnet but it doesnt create the table again[SHC ISSUE- https://github.com/hortonworks-spark/shc/issues/151].If we take the master branch and build it ,we dont need this parameter.
  private def writeWithCatalog(dataFrame: DataFrame, catalog: String, hbaseConfigFileLocation: String) = {
    try {
      dataFrame
        .write
        .options(Map((HBaseTableCatalog.tableCatalog, catalog), (HBaseTableCatalog.newTable, "5"), (HBaseRelation.HBASE_CONFIGFILE, hbaseConfigFileLocation)))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * This function writes data to HBase with catalog string.
    *
    * @param dataFrame DataFrame to write to HBase.
    * @param catalog   Catalog string holdin schema for HBase table.
    */
  // HBaseTableCatalog.newTable Property needs to be set as a default paramaneter as SHC Connector expects this argumnet but it doesnt create the table again[SHC ISSUE- https://github.com/hortonworks-spark/shc/issues/151].If we take the master branch and build it ,we dont need this parameter.
  private def writeWithCatalog(dataFrame: DataFrame, catalog: String) = {
    try {
      dataFrame
        .write
        .options(Map((HBaseTableCatalog.tableCatalog, catalog), (HBaseTableCatalog.newTable, "5")))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

}
