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

package com.paypal.gimel.hbase

import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.hbase.conf.{HbaseConfigs, HbaseConstants}
import com.paypal.gimel.hbase.utilities.{HBaseLookUp, HBasePut, HBaseSparkConnector, HBaseUtilities}
import com.paypal.gimel.logger.Logger

/**
  * Concrete Implementation for HBASE Dataset
  *
  * @param sparkSession : SparkSession
  */

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  /**
    * Change this parameter with cluster config
    */
  logger.info(s"Initiated --> ${this.getClass.getName}")
  lazy val hbaseUtilities = HBaseUtilities(sparkSession)
  lazy val hbaseLookUp = HBaseLookUp(sparkSession)
  lazy val hbasePut = HBasePut(sparkSession)
  lazy val hbaseSparkConnector = HBaseSparkConnector(sparkSession)

  /**
    *
    * @param dataset Name of the UDC Data Set
    * @param dataSetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to get 10 factor parallelism (specifically)
    *                val props = Map("coalesceFactor" -> 10)
    *                val data = Dataset(sc).read("flights", props)
    *                data.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */
  override def read(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {
    if (dataSetProps.isEmpty) throw new DataSetException("props cannot be empty !")

    val hbaseOperation = dataSetProps.getOrElse(HbaseConfigs.hbaseOperation, HbaseConstants.SCAN_OPERATION).toString
    hbaseOperation match {
      case HbaseConstants.GET_OPERATION =>
        logger.info("Reading through Java Get API.")
        hbaseLookUp.get(dataset, dataSetProps)
      case _ =>
        logger.info("Reading through SHC Connector.")
        hbaseSparkConnector.read(dataset, dataSetProps)
    }
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataFrame The Dataframe to write into Target
    * @param dataSetProps
    *                  Example Usecase : we want only 1 executor for hbase (specifically)
    *                  val props = Map("coalesceFactor" -> 1)
    *                  Dataset(sc).write(clientDataFrame, props)
    *                  Inside write implementation :: dataFrame.coalesce(props.get("coalesceFactor"))
    * @return DataFrame
    */

  override def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    if (dataSetProps.isEmpty) {
      throw new DataSetException("props cannot be empty !")
    }

    val castedDataFrame = hbaseUtilities.castAllColsToString(dataFrame)
    val hbaseOperation = dataSetProps.getOrElse(HbaseConfigs.hbaseOperation, HbaseConstants.SCAN_OPERATION).toString
    hbaseOperation match {
      case HbaseConstants.PUT_OPERATION =>
        logger.info("Writing through Java Put API.")
        hbasePut.put(dataset, castedDataFrame, dataSetProps)
      case _ =>
        logger.info("Writing through SHC Connector.")
        hbaseSparkConnector.write(dataset, castedDataFrame, dataSetProps)
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
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new UnsupportedOperationException(s"DataSet create for hbase currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new UnsupportedOperationException(s"DataSet drop for hbase currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new UnsupportedOperationException(s"DataSet truncate for hbase currently not Supported")
  }

  /**
    * Save Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for Hbase Dataset")
  }

  /**
    * Clear Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for Hbase Dataset")
  }
}

/**
  * Custom Exception for HBase API initiation errors
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class DataSetException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}
