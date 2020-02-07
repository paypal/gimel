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

import com.paypal.gimel.common.storageadmin.HBaseAdminClient
import com.paypal.gimel.hbase.conf.{HbaseClientConfiguration, HbaseConfigs, HbaseConstants}
import com.paypal.gimel.logger.Logger

/**
  * Spark Hbase Connector by Hortonworks implementations internal to Gimel
  */
object HBaseSparkConnector {

  def apply(sparkSession: SparkSession): HBaseSparkConnector = new HBaseSparkConnector(sparkSession)

}

class HBaseSparkConnector(sparkSession: SparkSession) {
  val logger = Logger()
  lazy val hbaseUtilities = HBaseUtilities(sparkSession)

  /**
    * This function performs scan/bulkGet on hbase table
    *
    * @param dataset Name
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

      val conf = new HbaseClientConfiguration(dataSetProps)
      // Setting the map (Column family -> Array of columns)
      val columnFamilyToColumnMapping: Map[String, Array[String]] = hbaseUtilities.getColumnMappingForColumnFamily(conf.hbaseNameSpace,
        conf.hbaseTableName,
        conf.hbaseTableColumnMapping,
        conf.maxSampleRecordsForSchema,
        conf.maxColumnsForSchema)
      logger.info("Column mapping -> " + columnFamilyToColumnMapping)
      // Get the hbase-site.xml file location
      val hbaseConfigFileLocation = HBaseAdminClient.getHbaseSiteXml(conf.hbaseSiteXMLHDFSPath)
      // Create catalog for the SHC connector
      val catalog = HBaseCatalog(conf.hbaseNameSpace, conf.hbaseTableName, columnFamilyToColumnMapping, conf.hbaseRowKeys,
        "PrimitiveType", conf.hbaseColumnNamewithColumnFamilyAppended)
      logger.info(s"Reading with catalog --> $catalog")

      val dataframe = conf.hbaseSiteXMLHDFSPath match {
        case HbaseConstants.NONE_STRING =>
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
    * @param dataset Name
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
      val conf = new HbaseClientConfiguration(dataSetProps)

      if (conf.hbaseRowKeys.diff(dataFrame.columns.toSeq).nonEmpty) {
        throw new IllegalArgumentException(
          s"""
             |Row Key columns not found in input dataframe.
             |You can modify the value through ${HbaseConfigs.hbaseRowKey} parameter.
             |Note: Default value is first column of the schema from UDC or ${HbaseConstants.DEFAULT_ROW_KEY_COLUMN}.
             |""".stripMargin)
      }
      // Get columns in dataframe excluding row key columns
      val dfColumns = dataFrame.columns.filter(x => !conf.hbaseRowKeys.contains(x)).toSeq
      logger.info("Columns in dataframe -> " + dfColumns)
      // Setting (Column family -> array of columns) mapping
      val columnFamilyToColumnMapping: Map[String, Array[String]] = hbaseUtilities.getColumnMappingForColumnFamily(conf.hbaseNameSpace,
        conf.hbaseTableName,
        conf.hbaseTableColumnMapping,
        conf.maxSampleRecordsForSchema,
        conf.maxColumnsForSchema)
      logger.info("Column mapping -> " + columnFamilyToColumnMapping)
      val columnsInSchema = columnFamilyToColumnMapping.map(_._2).flatten.toSeq
      logger.info("Columns in schema : " + columnsInSchema)
      // Check what columns in the input hbase column mapping are not present in the input dataframe
      val diff = columnsInSchema.diff(dfColumns)
      if (diff.nonEmpty) {
        throw new IllegalArgumentException(
          s"""
             |Columns : ${diff.mkString(",")} not found in dataframe schema.
             |Please check the property : ${HbaseConfigs.hbaseColumnMappingKey} = ${conf.hbaseTableColumnMapping}
             |""".stripMargin
        )
      }
      // Select columns provided in gimel.hbase.column.mapping property and row keys from the input dataframe.
      val dataFrameToWrite = dataFrame.selectExpr(columnsInSchema ++ conf.hbaseRowKeys: _*)
      // Get the hbase-site.xml file location
      val hbaseConfigFileLocation = HBaseAdminClient.getHbaseSiteXml(conf.hbaseSiteXMLHDFSPath)
      // Create catalog for the SHC connector
      val catalog = HBaseCatalog(conf.hbaseNameSpace, conf.hbaseTableName, columnFamilyToColumnMapping, conf.hbaseRowKeys, "PrimitiveType", false)
      logger.info(s"Writing with catalog --> $catalog")
      conf.hbaseSiteXMLHDFSPath match {
        case HbaseConstants.NONE_STRING =>
          logger.info(s"PLAIN WRITE")
          writeWithCatalog(dataFrameToWrite, catalog)
        case _ =>
          logger.info(s"write with ${conf.hbaseSiteXMLHDFSPath}")
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
