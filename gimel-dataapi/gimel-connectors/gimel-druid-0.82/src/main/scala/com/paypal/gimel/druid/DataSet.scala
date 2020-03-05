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

package com.paypal.gimel.druid

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.druid.conf.{DruidClientConfiguration, DruidConfigs, DruidConstants}
import com.paypal.gimel.druid.util.DruidUtility
import com.paypal.gimel.druid.writer.DruidRealtimeWriter
import com.paypal.gimel.logger.Logger

/**
  * Concrete Implementation for Druid DataSet.
  *
  * @param sparkSession : SparkSession
  */

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  /**
    * Read Implementation for Casandra DataSet.
    *
    * @param dataset      Name of the UDC Data Set.
    * @param datasetProps Additional parameters for read and write operations in DataSet class.
    * @return DataFrame
    */
  override def read(dataset: String, datasetProps: Map[String, Any]): DataFrame = {
    throw new Exception("Read for Druid Dataset is not enabled.")
  }

  /** Write Implementation for Druid DataSet.
    *
    * @param dataset      Name of the UDC Data Set.
    * @param dataFrame    The DataFrame to write to target.
    * @param datasetProps Additional parameters for read and write operations in DataSet class.
    * @return DataFrame
    */

  override def write(dataset: String, dataFrame: DataFrame,
                     datasetProps: Map[String, Any]): DataFrame = {
    logger.info(s"Druid Dataset Write Initialized for ---> $dataset.")
    logger.info(s"Scala Version Used ---> ${scala.util.Properties.versionString}")

    if (datasetProps.isEmpty) {
      throw new DataSetException("Props Map Cannot be emtpy for DruidDataSet Write.")
    }

    val allProps = datasetProps ++
      Map(DruidConfigs.FIELDS -> DruidUtility.getFieldNames(dataFrame))

    logger.info(s"Begin Building DruidClientConfiguration")
    logger.debug(s"Incoming Properties --> ${
      allProps.map(x => s"${x._1} -> ${x._2}")
        .mkString("\n")
    }")

    val conf = new DruidClientConfiguration(allProps)

    logger.debug(s"DruidClientConfiguration --> $conf")
    logger.info(s"DruidClientConfiguration Building done --> " +
      s"${conf.getClass.getName}")

    // Get load type from DruidClientConfiguration.
    // i.e Real-time or Batch and then runs appropriate driver.
    // Defaults to Real-time Driver.
    conf.druidLoadType match {
      case DruidConstants.REALTIME_LOAD =>
        DruidRealtimeWriter.writeToTable(sparkSession, conf, dataFrame)

      case DruidConstants.BATCH_LOAD =>
        val errorMsg = "Batch Load type for druid-connector has not been implemented."
        throw new IllegalArgumentException(errorMsg)

      case _ =>
        DruidRealtimeWriter.writeToTable(sparkSession, conf, dataFrame)
    }

    dataFrame
  }

  // Add Additional Supported types to this list as and when we support other Types of RDD
  // Example to support RDD[String], add to List <typeOf[Seq[Map[String, String]]].toString)>
  override val supportedTypesOfRDD: List[String] = List(typeOf[Map[String, Any]].toString)

  /**
    * Writes a given dataframe to the actual target System.
    * (Example Hive : DB.Table | HBASE namespace.Table)
    *
    * The inheriting DataSet Operators must typeCast the RDD to supported types.
    *
    *
    * <li> instance#1:
    * ElasticSearchDataSet may support just RDD[Seq(Map[String, String])],
    * so Elastic Search must implement supported Type checking
    *
    * <li> instance#2:  Kafka, HDFS, HBASE throw Unsupported Operation Exception.
    * The exception should clearly educate users—Until they support an RDD operation for Any Type T.
    *
    * Additional parameters for read and write operations in DataSet class
    * Example: to write kafka with a specific parallelism:
    * {{{
    * val props = Map("parallelsPerPartition" -> 10)
    * Dataset(sc).write(clientDataFrame, props)
    * }}}
    *
    * @param dataset      Name of the UDC Data Set.
    * @param rdd          The RDD[T] to write into Target.
    * @param datasetProps Map containing dataset props
    * @return RDD[T]
    */
  def write[T: TypeTag](dataset: String, rdd: RDD[T], datasetProps: Map[String, Any]): RDD[T] = {
    logger.info(s"Druid Dataset Write Initialized for ---> $dataset.")
    logger.info(s"Scala Version Used ---> ${scala.util.Properties.versionString}")

    if (!supportedTypesOfRDD.contains(typeOf[T].toString)) {
      throw new UnsupportedOperationException(
        s"""Invalid RDD Type. Supported Types :
            |${supportedTypesOfRDD.mkString(" | ")}""".stripMargin)
    }

    if (datasetProps.isEmpty) {
      throw new DataSetException("Props Map Cannot be emtpy for DruidDataSet Write.")
    }

    val allProps = datasetProps ++
      Map(DruidConfigs.FIELDS -> DruidUtility.getFieldNames(dataset, sparkSession))

    logger.info(s"Begin Building DruidClientConfiguration")
    logger.debug(s"Incoming Properties --> ${
      allProps.map(x => s"${x._1} -> ${x._2}")
        .mkString("\n")
    }")

    val conf = new DruidClientConfiguration(allProps)

    logger.debug(s"DruidClientConfiguration --> $conf")
    logger.info(s"DruidClientConfiguration Building done --> " +
      s"${conf.getClass.getName}")

    // Get load type from DruidClientConfiguration.
    // i.e Real-time or Batch and then runs appropriate driver.
    // Defaults to Real-time Driver.
    conf.druidLoadType match {
      case DruidConstants.REALTIME_LOAD =>
        DruidRealtimeWriter.writeToTable(sparkSession, conf, rdd.asInstanceOf[RDD[Map[String, Any]]])

      case DruidConstants.BATCH_LOAD =>
        val errorMsg = "Batch Load type for druid-connector has not been implemented."
        throw new IllegalArgumentException(errorMsg)

      case _ =>
        DruidRealtimeWriter.writeToTable(sparkSession, conf, rdd.asInstanceOf[RDD[Map[String, Any]]])
    }

    rdd
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet create for druid currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet drop for druid currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet truncate for druid currently not Supported")
  }

  /**
    * Save Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for Druid Dataset")
  }

  /**
    * Clear Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for Druid Dataset")
  }
}

/**
  * Custom Exception for DruidDataset initiation errors
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
