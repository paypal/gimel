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

package com.paypal.gimel.hive

import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.hive.utilities.HiveUtils
import com.paypal.gimel.logger.Logger

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  val hiveUtils = new HiveUtils

  /**
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
    if (dataSetProps.isEmpty) {
      throw new DataSetOperationException("Props Cannot Be Empty !")
    }
    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]

    val checkPCatalogDB = hiveUtils.checkIfCatalogTable(dataset);
    if (checkPCatalogDB) {
      if (!hiveUtils.isCrossCluster(datasetProps)) {
        val hiveDataSetName = datasetProps.props(GimelConstants.HIVE_DATABASE_NAME) + "." + datasetProps.props(GimelConstants.HIVE_TABLE_NAME)
        sparkSession.read.table(hiveDataSetName)
      } else {
        logger.info("Cross Cluster Read Detected !")
        val hdfsDataSet = new com.paypal.gimel.hdfs.DataSet(sparkSession)
        hdfsDataSet.read(dataset, dataSetProps)
      }
    } else {
      sparkSession.read.table(dataset)
    }
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataFrame The DataFrame to Write into Target
    * @param dataSetProps
    *                  props is the way to set various additional parameters for read and write operations in DataSet class
    *                  Example Usecase : we want to write only 1 file per executor
    *                  val props = Map("coalesceFactor" -> 1)
    *                  Dataset(sc).write(clientDataFrame, props)
    *                  Inside write implementation :: dataFrame.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */

  override def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    if (dataSetProps.isEmpty) {
      throw new DataSetOperationException("Props Cannot Be Empty!")
    }
    val dataSet: String = dataSetProps(GimelConstants.RESOLVED_HIVE_TABLE).toString
    hiveUtils.write(dataSet, dataFrame, sparkSession, dataSetProps)
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
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

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
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    hiveUtils.create(dataset, dataSetProps, sparkSession)
  }

  /**
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    hiveUtils.drop(dataset, dataSetProps, sparkSession)
  }

  /**
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    hiveUtils.truncate(dataset, dataSetProps, sparkSession)
  }

  /**
    * Save Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for Hive Dataset")
  }

  /**
    * Clear Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for Hive Dataset")
  }
}

/**
  * Custom Exception for errors for Hive Read and Write operations
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
