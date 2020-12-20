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

import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.hdfs.conf.{HdfsClientConfiguration, HdfsConfigs, HdfsConstants}
import com.paypal.gimel.hdfs.utilities.HDFSUtilities
import com.paypal.gimel.logger.Logger

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  val hdfsUtils = new HDFSUtilities
  import hdfsUtils._

  // GET LOGGER
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  private var conf: HdfsClientConfiguration = _

  /** Read Implementation for HDFS DataSet
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to get 10 factor parallelism (specifically)
    *                val props = Map("coalesceFactor" -> 10)
    *                val data = Dataset(sc).read("flights", props)
    *                Inside write implementation :: dataFrame.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */
  override def read(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {
    try {
      if (dataSetProps.isEmpty) {
        throw new DataSetOperationException("Props Cannot Be Empty !")
      }
      conf = new HdfsClientConfiguration(dataSetProps)
      val clusterReadOptions: Map[String, String] = getOptions(conf.readOptions)
      val clusterDataLocationPath = new java.net.URI(conf.clusterDataLocation).getPath()
      val clusterPathToRead = conf.clusterNameNode + "/" + clusterDataLocationPath
      logger.info("Read options provided for the cluster are: " + clusterReadOptions)

      if (clusterPathToRead.toLowerCase.startsWith(HdfsConstants.alluxioString)) {
        val (isThresholdMet, dataSizeGB) = validateThreshold(sparkSession, clusterPathToRead, conf.clusterThresholdGB, conf.clusterNameNode)
        logger.info("Cluster Path --> " + clusterPathToRead)
        logger.info(s"Size of the Data in the above path -->${dataSizeGB} GB")
        logger.info(s"WARNING : Size of the data in the root path before applying where clause(if any)  -->${dataSizeGB} GB")
      }
      readPath(sparkSession,
        conf,
        clusterPathToRead,
        conf.clusterdataFormat,
        conf.inferSchema,
        conf.header,
        conf.datasetProps.fields,
        conf.datasetProps.partitionFields,
        clusterReadOptions)
    }
    catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw new DataSetOperationException(s"Read Error for ${dataset} with Props ${dataSetProps}")
    }

  }

  /** Get user custom options for read in Hdfs
    *
    * @param value      The Json String from the dataset properties
    * @return Map[String,String]
    */
  def getOptions(value: String): Map[String, String] = {
    implicit val formats = org.json4s.DefaultFormats
    if(!(value.compareTo("") == 0)) {
      logger.info("value is " + value)
      parse(value).extract[Map[String, String]]
    }
    Map[String, String]()

  }

  /** Write Implementation for HDFS DataSet
    *
    * @param dataset      Name of the UDC Data Set
    * @param dataFrame    The DataFrame to Write into Target
    * @param dataSetProps props is the way to set various additional parameters for read and write operations in DataSet class
    * @return DataFrame
    */

  override def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    if (dataSetProps.isEmpty) {
      throw new DataSetOperationException("Props Cannot Be Empty")
    }
    try {
      writeToHDFS(dataFrame, sparkSession, dataSetProps)
      logger.info("Finished")
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
    * @param dataset Name of the UDC Data Set
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
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new UnsupportedOperationException(s"DataSet create for hdfs/hive currently not Supported")
  }

  /**
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new UnsupportedOperationException(s"DataSet drop for hdfs/hive currently not Supported")
  }

  /**
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new UnsupportedOperationException(s"DataSet truncate for hdfs/hive currently not Supported")
  }

  /**
    * Save Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for HDFS Dataset")
  }

  /**
    * Clear Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for HDFS Dataset")
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
