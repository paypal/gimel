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

package com.paypal.gimel.hdfs

import java.net._

import scala.reflect.runtime.universe._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{CatalogProviderConstants, GimelConstants}
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.hdfs.conf.{HdfsConfigs, HdfsConstants}
import com.paypal.gimel.hdfs.utilities.HDFSUtilities
import com.paypal.gimel.logger.Logger

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  /** Read Implementation for HDFS DataSet
    *
    * @param dataset Name of the PCatalog Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to get 10 factor parallelism (specifically)
    *                val props = Map("coalesceFactor" -> 10)
    *                val data = Dataset(sc).read("flights", props)
    *                Inside write implementation :: dataFrame.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */
  override def read(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {
    if (dataSetProps.isEmpty) {
      throw new DataSetOperationException("Props Cannot Be Empty !")
    }
    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val dataSet: String = dataSetProps(GimelConstants.RESOLVED_HIVE_TABLE).toString
    val dataLocation = datasetProps.props(CatalogProviderConstants.PROPS_LOCATION)
    val dataFormat = datasetProps.props.getOrElse(HdfsConfigs.hdfsDataFormatKey, "None")
    val inferSchema = dataSetProps.getOrElse(HdfsConfigs.inferSchemaKey, "true").toString
    val header = dataSetProps.getOrElse(HdfsConfigs.fileHeaderKey, "true").toString
    val isCrossClusterDataSet = datasetProps.props.contains(HdfsConfigs.hdfsStorageNameKey)
    if (!isCrossClusterDataSet) {
      try {
        dataFormat.toLowerCase match {
          case "csv" =>
            val sqlContext = sparkSession.sqlContext
            sqlContext.read.option(HdfsConstants.inferSchema, inferSchema).option(HdfsConstants.fileHeader, header).csv(dataLocation)
          case _ =>
            sparkSession.sql(s"select * from ${dataSet}")
        }
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          throw new DataSetOperationException(s"Read Error for ${dataset} with Props ${dataSetProps}")
      }
    }
    else {

      logger.info("Cross Cluster Read Detected !")

      val crossClusterName = datasetProps.props.getOrElse(HdfsConfigs.hdfsStorageNameKey, "")
      val crossClusterNameNode = datasetProps.props.getOrElse(HdfsConfigs.hdfsNameNodeKey, "")
      val crossClusterDataLocation = datasetProps.props.getOrElse(HdfsConfigs.hdfsDataLocationKey, "")
      val crossClusterdataFormat = datasetProps.props.getOrElse(HdfsConfigs.hdfsDataFormatKey, "")
      val crossClusterThresholdGB = dataSetProps.getOrElse(HdfsConfigs.hdfsCrossClusterThresholdKey, HdfsConstants.thresholdGBData).toString
      val crossClusterpathToRead = crossClusterNameNode + "/" + crossClusterDataLocation

      // TODO: Threshold Logic will be implemented with respect to partition
      if (!crossClusterpathToRead.toLowerCase.startsWith(HdfsConstants.alluxioString)) {
        val (isThresholdMet, dataSizeGB) = validateThreshold(crossClusterpathToRead, crossClusterThresholdGB, crossClusterNameNode)
        // if (isThresholdMet) {
        logger.info("CrossCluster Path --> " + crossClusterpathToRead)
        logger.info(s"Size of the Data in the above path -->${dataSizeGB} GB")
        // This will be removed once logger issue is fixed
        println("CrossCluster Path --> " + crossClusterpathToRead)
        println(s"WARNING : Size of the data in the root path before applying where clause(if any)  -->${dataSizeGB} GB")
      }
      readAcrossCluster(crossClusterpathToRead, crossClusterdataFormat, inferSchema, header)
      // } else {
      //   throw new DataSetOperationException(s"Size of the data is greater than threshold GB  => Data size GB ${dataSizeGB} > threshold GB ${crossClusterThresholdGB}")
      // }
    }
  }

  /** Write Implementation for HDFS DataSet
    *
    * @param dataset      Name of the PCatalog Data Set
    * @param dataFrame    The DataFrame to Write into Target
    * @param dataSetProps props is the way to set various additional parameters for read and write operations in DataSet class
    * @return DataFrame
    */

  override def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    if (dataSetProps.isEmpty) {
      throw new DataSetOperationException("Props Cannot Be Empty")
    }
    try {
      val dataSet: String = dataSetProps(GimelConstants.RESOLVED_HIVE_TABLE).toString
      val hdfsUtils = new HDFSUtilities
      hdfsUtils.writeToHDFS(dataSet, dataFrame, sparkSession, dataSetProps)
      dataFrame
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new DataSetOperationException(s"Write Error for ${dataset} with Props ${dataSetProps}")
    }
  }

  // Add Additional Supported types to this list as and when we support other Types of RDD
  // Example to start supporting RDD[String], add to List <  typeOf[Seq[Map[String, String]]].toString)  >
  override val supportedTypesOfRDD: List[String] = List[String]()

  /**
    * Function writes a given dataframe to the actual Target System (Example Hive : DB.Table | HBASE namespace.Table)
    *
    * @param dataset Name of the PCatalog Data Set
    * @param rdd     The RDD[T] to write into Target
    *                Note the RDD has to be typeCast to supported types by the inheriting DataSet Operators
    *                instance#1 : ElasticSearchDataSet may support just RDD[Seq(Map[String, String])], so Elastic Search must implement supported Type checking
    *                instance#2 : Kafka, HDFS, HBASE - Until they support an RDD operation for Any Type T : They throw Unsupporter Operation Exception & Educate Users Clearly !
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to write kafka with a specific parallelism : One can set something like below -
    *                val props = Map("parallelsPerPartition" -> 10)
    *                Dataset(sc).write(clientDataFrame, props)
    * @return RDD[T]
    */
  def write[T: TypeTag](dataset: String, rdd: RDD[T], dataSetProps: Map[String, Any]): RDD[T] = {

    if (!supportedTypesOfRDD.contains(typeOf[T].toString)) {
      throw new UnsupportedOperationException(s"""Invalid RDD Type. Supported Types : ${supportedTypesOfRDD.mkString(" | ")}""")
    } else {
      // todo Implementation for Write
      rdd
    }
  }

  /**
    * Function reads  cross cluster data and return as a dataframe
    * @param pathToRead             Path of HDFS Data
    * @param crossClusterDataFormat Cross Cluster Format
    * @return DataFrame
    */
  def readAcrossCluster(pathToRead: String, crossClusterDataFormat: String, inferSchema: String, header: String): DataFrame = {
    val crossClusterData = crossClusterDataFormat.toLowerCase match {
      case "text" =>
        val k = sparkSession.sparkContext.textFile(pathToRead)
        val kRow = k.map(Row(_))
        val kDataFrame = sparkSession.createDataFrame(kRow, StructType(Seq(StructField(GimelConstants.PCATALOG_STRING, StringType))))
        kDataFrame
      case "parquet" =>
        sparkSession.read.parquet(pathToRead)
      case "orc" =>
        sparkSession.read.orc(pathToRead)
      case "csv" =>
        val sqlContext = sparkSession.sqlContext
        sqlContext.read.option(HdfsConstants.inferSchema, inferSchema).option(HdfsConstants.fileHeader, header).csv(pathToRead)
      case _ =>
        throw new Exception(s"UnSupported Format ${crossClusterDataFormat}")
    }
    crossClusterData
  }

  /**
    * Function reads  cross cluster data and return as a dataframe
    * @param path        Path of HDFS Data
    * @param thresholdGB Threshold GB to Read
    * @return isThresholdMet,sizeGB to String
    */

  def validateThreshold(path: String, thresholdGB: String, uri: String): (Boolean, String) = {
    val conf = new org.apache.hadoop.conf.Configuration()
    val fs1 = FileSystem.get(new URI(uri), conf)
    val hdfsPath = new Path(path)
    val contentSummary = fs1.getContentSummary(hdfsPath)
    val gb = (1024 * 1024 * 1024).toDouble
    val size = contentSummary.getLength
    val sizeGB = size.toDouble / gb
    val isThresholdMet = sizeGB <= thresholdGB.toDouble
    (isThresholdMet, sizeGB.toString)
  }

  /**
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Boolean = {
    throw new Exception(s"DataSet create for hdfs/hive currently not Supported")
    true
  }

  /**
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Boolean = {
    throw new Exception(s"DataSet drop for hdfs/hive currently not Supported")
    true
  }

  /**
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Boolean = {
    throw new Exception(s"DataSet truncate for hdfs/hive currently not Supported")
    true
  }
}


/**
  * Custom Exception for errors for HDFS Read and Write operations
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class DataSetOperationException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}

