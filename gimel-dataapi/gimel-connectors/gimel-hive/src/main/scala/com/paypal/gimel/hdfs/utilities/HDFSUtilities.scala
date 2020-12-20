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

import java.net.URI

import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.paypal.gimel.common.catalog.{DataSetProperties, Field}
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.hdfs.conf.{HdfsClientConfiguration, HdfsConfigs, HdfsConstants}
import com.paypal.gimel.logger.Logger

class HDFSUtilities {
  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val fs: FileSystem = FileSystem.get(hadoopConf)
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  /**
    * Function reads  cross cluster data and return as a dataframe
    *
    * @param pathToRead             Path of HDFS Data
    * @param clusterDataFormat Cross Cluster Format
    * @return DataFrame
    */
  def readPath(sparkSession: SparkSession, conf: HdfsClientConfiguration, pathToRead: String, clusterDataFormat: String,
               inferSchema: String, header: String, fields: Array[Field], partitionFields: Array[Field],
               clusterReadOptions: Map[String, String]): DataFrame = {
    import sparkSession.sqlContext.implicits._
    logger.info("Reading from the specified cluster")
    val clusterData = clusterDataFormat.toLowerCase match {
      case "text" =>
        val dataFrame = sparkSession.read.text(pathToRead)
        if ((conf.rowDelimiter != "" || conf.colDelimiter != "") && fields != null) {
          deserializeTextFileData(dataFrame, conf.colDelimiter, conf.rowDelimiter, fields)
        } else {
          dataFrame
        }
      case "parquet" =>
        sparkSession.read.options(clusterReadOptions).parquet(pathToRead)
      case "orc" =>
        sparkSession.read.options(clusterReadOptions).orc(pathToRead)
      case "sequence" =>
        val dataFrame = sparkSession.sparkContext.sequenceFile(pathToRead, classOf[Text], classOf[Text]).map { case (_, y) => (y.toString) }.toDF()
        deserializeSequenceFileData(dataFrame, conf.colDelimiter, fields, partitionFields)
      case "csv" =>
        sparkSession.read.option(HdfsConstants.delimiter, conf.csvDelimiter).option(HdfsConstants.inferSchema, inferSchema).option(HdfsConstants.fileHeader, header).options(clusterReadOptions).csv(pathToRead)
      case "avro" =>
        val dataFrame = sparkSession.read.options(clusterReadOptions).format(HdfsConstants.avroInputFormat).load(path = pathToRead)
        dataFrame
      case "json" =>
        val dataFrame = sparkSession.read.options(clusterReadOptions).json(pathToRead)
        dataFrame
      case "gzip" =>
        val k = sparkSession.sparkContext.textFile(pathToRead)
        val kRow = k.map(Row(_))
        val kDataFrame = sparkSession.createDataFrame(kRow, StructType(Seq(StructField(GimelConstants.PCATALOG_STRING, StringType))))
        kDataFrame
      case "zip" =>
        val k = sparkSession.sparkContext.textFile(pathToRead)
        val kRow = k.map(Row(_))
        val kDataFrame = sparkSession.createDataFrame(kRow, StructType(Seq(StructField(GimelConstants.PCATALOG_STRING, StringType))))
        kDataFrame
      case _ =>
        throw new IllegalArgumentException(s"UnSupported Format ${clusterDataFormat}")
    }
    clusterData
  }

  /**
    * Function to validate the read size threshold
    *
    * @param path        Path of HDFS Data
    * @param thresholdGB Threshold GB to Read
    * @return isThresholdMet,sizeGB to String
    */

  def validateThreshold(sparkSession: SparkSession, path: String, thresholdGB: String, uri: String): (Boolean, String) = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs1 = FileSystem.get(new URI(uri), conf)
    val hdfsPath = new Path(path)
    logger.info("hdfs path specified for validate threshold is:" + hdfsPath)
    val contentSummary = fs1.getContentSummary(hdfsPath)
    val gb = (1024 * 1024 * 1024).toDouble
    val size = contentSummary.getLength
    val sizeGB = size.toDouble / gb
    val isThresholdMet = sizeGB <= thresholdGB.toDouble
    (isThresholdMet, sizeGB.toString)
  }

  /**
    *
    * @param dataFrame    : dataframe to read data from
    * @param dataSetProps : additional parameters passed to data write API - hdfsPath, file format, compression, delimiter for text/sequence-files
    * @return : same source dataframe
    */
  def writeToHDFS(dataFrame: DataFrame, sparkSession: SparkSession, dataSetProps: Map[String, Any]): Unit = {

    val sc = sparkSession.sparkContext
    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val hdfsPath = datasetProps.props.getOrElse(HdfsConfigs.hdfsDataLocationKey, "")
    val inDataFormat = dataSetProps.getOrElse(HdfsConfigs.hdfsDataFormatKey, "").toString
    val compressionCodec = dataSetProps.getOrElse(HdfsConfigs.compressionCodecKey, "SNAPPY").toString.toLowerCase
    val delimiter = dataSetProps.getOrElse(HdfsConfigs.columnDelimiter, HdfsConstants.tabDelimiterValue).toString
    val saveMode = dataSetProps.getOrElse(HdfsConfigs.saveModeInfo, SaveMode.Overwrite).toString.toLowerCase
    val partitionColumns = datasetProps.partitionFields.map(_.fieldName)

    try {
      if (saveMode.toUpperCase == "APPEND" & (inDataFormat.toUpperCase == "TEXT" | inDataFormat.toUpperCase == "SEQUENCEFILE")) {
        logger.info(s"Unsupported saveMode $saveMode for input file format $inDataFormat")
        logger.info(s"Supports saveMode-Append/Overwrite for input file format PARQUET/ORC and saveMode-Overwrite for TEXT/SEQUENCEFILE")
        throw new UnsupportedOperationException(s"Unsupported saveMode $saveMode for input file format $inDataFormat")
      }

      inDataFormat.toLowerCase match {
        case "text" =>
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
        case "parquet" =>
          val sqlContext = sparkSession.sqlContext
          sqlContext.setConf(HdfsConfigs.sparkSqlCompressionCodec, compressionCodec)
          if (partitionColumns.isEmpty) {
            dataFrame.write.mode(saveMode).format("parquet").save(hdfsPath)
          } else {
            dataFrame.write.partitionBy(partitionColumns: _*).mode(saveMode).format("parquet").save(hdfsPath)
          }
        case "orc" =>
          dataFrame.registerTempTable("temptableorc")
          val orcDF = sparkSession.sql(s"select * from temptableorc")
          if (partitionColumns.isEmpty) {
            orcDF.write.mode(saveMode).format("orc").save(hdfsPath)
          } else {
            orcDF.write.partitionBy(partitionColumns: _*).mode(saveMode).format("orc").save(hdfsPath)
          }
        case "csv" =>
          logger.info("writing CSV with saveMode = " + saveMode + " and path is " + hdfsPath)
          dataFrame.write.mode(saveMode).option(HdfsConstants.fileHeader, "true").csv(hdfsPath)
          dataFrame.write.format(HdfsConstants.csvInputFormat).mode(saveMode).option(HdfsConstants.fileHeader, "true").save(hdfsPath)
        case "json" =>
          logger.info("writing json file to path -> " + hdfsPath + " in mode -> " + saveMode)
          dataFrame.write.mode(saveMode).json(hdfsPath)
        case "avro" =>
          logger.info("writing avro file to path -> " + hdfsPath + " in mode -> " + saveMode)
          if (partitionColumns.isEmpty) {
            dataFrame.write.mode(saveMode).format(HdfsConstants.avroInputFormat).save(hdfsPath)
          } else {
            dataFrame.write.partitionBy(partitionColumns: _*).mode(saveMode).format(HdfsConstants.avroInputFormat).save(hdfsPath)
          }
        case "sequence" =>
          throw new UnsupportedOperationException(s"Sequence File is currently not Supported")
      }
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Function deserializes sequence files by a delimiter
    *
    * @param dataFrame Input Dataframe read from sequence file
    * @param delimiter Delimiter by which sequence file is separated
    * @return Dataframe
    */
  def deserializeSequenceFileData(dataFrame: DataFrame, delimiter: String, fields: Array[Field], partitionFields: Array[Field]): DataFrame = {
    logger.info("Provided delimiter for sequence file is " + StringEscapeUtils.escapeJava(delimiter))
    val dataFrameTmp = dataFrame.withColumn("_tmp", split(col("value").cast(StringType), delimiter))
    dataFrameTmp.select(fields.zipWithIndex.map {field =>
      col("_tmp").getItem(field._2).as(field._1.fieldName).cast(field._1.fieldType)}.toSeq: _*)
  }

  /**
    * Function deserializes sequence files by a delimiter
    *
    * @param dataFrame Input Dataframe read from sequence file
    * @param colDelimiter Column Delimiter by which fields in text file are separated
    * @param rowDelimiter row Delimiter by which rows in text file are separated
    * @param fields fields corresponding to schema fetched from UDC
    * @return Dataframe
    */
  def  deserializeTextFileData(dataFrame: DataFrame, colDelimiter: String, rowDelimiter: String, fields: Array[Field]): DataFrame = {
    val dataFrameRowArray = dataFrame.withColumn("_tmp", split(col("value").cast(StringType), rowDelimiter))
    val dataFrameSplitRow = dataFrameRowArray.withColumn("_tmp", explode(col("_tmp")))
    val dataFrameTmp = dataFrameSplitRow.withColumn("_tmp", split(col("_tmp").cast(StringType), colDelimiter))
    dataFrameTmp.select(fields.zipWithIndex.map {field =>
      col("_tmp").getItem(field._2).as(field._1.fieldName).cast(field._1.fieldType)}.toSeq: _*)

  }
}
