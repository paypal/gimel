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

package com.paypal.gimel.hdfs.utilities

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{CatalogProviderConstants, GimelConstants}
import com.paypal.gimel.hdfs.conf.{HdfsConfigs, HdfsConstants}
import com.paypal.gimel.logger.Logger

class HDFSUtilities {

  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val fs: FileSystem = FileSystem.get(hadoopConf)
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  /**
    *
    * @param dataset      : dataset name to be written
    * @param dataFrame    : dataframe to read data from
    * @param dataSetProps : additional parameters passed to data write API - hdfsPath, file format, compression, delimiter for text/sequence-files
    * @return : same source dataframe
    */
  def writeToHDFS(dataset: String, dataFrame: DataFrame, sparkSession: SparkSession, dataSetProps: Map[String, Any]): Unit = {

    val sc = sparkSession.sparkContext
    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val hdfsPath = dataSetProps.getOrElse(HdfsConfigs.hdfsDataLocationKey, "").toString
    val dataLocation = datasetProps.props(CatalogProviderConstants.PROPS_LOCATION)
    var inDataFormat = dataSetProps.getOrElse(HdfsConfigs.hdfsDataFormatKey, "").toString.toUpperCase
    if (!datasetProps.props(HdfsConfigs.hdfsDataFormatKey).isEmpty) {
      inDataFormat = datasetProps.props(HdfsConfigs.hdfsDataFormatKey);
    }
    val compressionCodec = dataSetProps.getOrElse(HdfsConfigs.compressionCodecKey, "").toString.toLowerCase
    // Column Delimiter is set to Default "Tab" if user is not providing delimiter for HDFS
    val columnDelimiter = dataSetProps.getOrElse(HdfsConfigs.columnDelimiter, HdfsConstants.tabDelimiterValue).toString.toLowerCase
    // Delimiter value is converted to Base 8 Octal Value and converted to string for writing into HDFS
    val delimiter: String = Character.toString(Integer.parseInt(columnDelimiter, Integer.parseInt(HdfsConstants.octalValue)).toChar)
    val saveMode = dataSetProps.getOrElse(HdfsConfigs.saveModeInfo, SaveMode.Overwrite).toString.toLowerCase
    val partitionColumns = datasetProps.partitionFields.map(_.fieldName)
    val header = dataSetProps.getOrElse(HdfsConfigs.fileHeaderKey, "true").toString

    try {
      if (saveMode.toUpperCase == "APPEND" & (inDataFormat.toUpperCase == "TEXT" | inDataFormat.toUpperCase == "SEQUENCEFILE")) {
        logger.info(s"Unsupported saveMode $saveMode for input file format $inDataFormat")
        logger.info(s"Supports saveMode-Append/Overwrite for input file format PARQUET/ORC and saveMode-Overwrite for TEXT/SEQUENCEFILE")
        throw new Exception(s"Unsupported saveMode $saveMode for input file format $inDataFormat")
      }

      inDataFormat.toUpperCase match {
        case "TEXT" =>
          val hdfsPathVar = new Path(hdfsPath)
          fs.delete(hdfsPathVar, true)
          if (compressionCodec.toUpperCase == "GZIP") {
            dataFrame.rdd.map(x => x.mkString(delimiter)).saveAsTextFile(hdfsPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
          } else if (compressionCodec.toUpperCase == "SNAPPY") {
            dataFrame.rdd.map(x => x.mkString(delimiter)).saveAsTextFile(hdfsPath, classOf[org.apache.hadoop.io.compress.SnappyCodec])
          } else if (compressionCodec.toUpperCase == "UNCOMPRESSED") {
            sc.hadoopConfiguration.set(HdfsConfigs.fileOutputFormatCompressionFlag, "false")
            dataFrame.rdd.map(x => x.mkString(delimiter)).saveAsTextFile(hdfsPath)
          }
        case "PARQUET" =>
          val sqlContext = sparkSession.sqlContext
          sqlContext.setConf(HdfsConfigs.sparkSqlCompressionCodec, compressionCodec)
          if (partitionColumns.isEmpty) {
            dataFrame.write.mode(saveMode).format("parquet").save(hdfsPath)
          } else {
            dataFrame.write.partitionBy(partitionColumns: _*).mode(saveMode).format("parquet").save(hdfsPath)
          }
        case "ORC" =>
          dataFrame.registerTempTable("temptableorc")
          val orcDF = sparkSession.sql(s"select * from temptableorc")
          if (partitionColumns.isEmpty) {
            orcDF.write.mode(saveMode).format("orc").save(hdfsPath)
          } else {
            orcDF.write.partitionBy(partitionColumns: _*).mode(saveMode).format("orc").save(hdfsPath)
          }
        case "CSV" =>
          dataFrame.write.mode(saveMode).option(HdfsConstants.fileHeader, "true").csv(dataLocation)
        case "SEQUENCEFILE" =>
          throw new Exception(s"Sequence File is currently not Supported")
      }
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }
}
