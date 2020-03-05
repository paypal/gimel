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

package com.paypal.gimel.jdbc

import scala.reflect.runtime.universe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.jdbc.utilities.JDBCUtilities
import com.paypal.gimel.logger.Logger

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")
  val jdbcUtilities = JDBCUtilities(sparkSession)

  /**
    * Reads the requested dataset with the given properties and returns a `DataFrame`.
    *
    * `dataSetProps` contains additional parameters for read and write operations.
    * Example:
    * For loading or exporting data from Teradata,
    * set "TYPE" parameter as "FASTLOAD" or "FASTEXPORT" (optional)
    *
    * @param dataset      The requested dataset.
    * @param dataSetProps The given properties
    * @return DataFrame
    */
  override def read(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {
    if (dataSetProps.isEmpty) {
      throw new DataSetOperationException("Props Cannot Be Empty!")
    }
    val dataSet = dataSetProps(GimelConstants.RESOLVED_HIVE_TABLE).toString
    try {
      jdbcUtilities.read(dataSet, dataSetProps)
    } catch {
      case e: Throwable =>
        val msg = s"Error reading from TERADATA table: ${e.getMessage}"
        throw new DataSetOperationException(msg, e)
    }
  }


  /**
    * Writes the given dataframe to the actual target JDBC Data Source.
    *
    * `dataSetProps` contains additional parameters for read and write operations.
    * Example:
    * For loading or exporting data from Teradata,
    * set "TYPE" parameter as "FASTLOAD" or "FASTEXPORT" (optional)
    *
    * @param dataset      The requested dataset.
    * @param dataFrame    The Dataframe to write into Target
    * @param dataSetProps The given properties
    * @return DataFrame
    */
  override def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    if (dataSetProps.isEmpty) {
      throw new DataSetOperationException("Props Cannot Be Empty!")
    }
    val dataSet = dataSetProps(GimelConstants.RESOLVED_HIVE_TABLE).toString
    try {
      jdbcUtilities.write(dataSet, dataFrame, dataSetProps)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw new DataSetOperationException(s"Error writing to TERADATA table", e)
    }
    dataFrame
  }

  /**
    * Writes the given dataframe to the actual target System.
    *
    * `dataSetProps` contains additional parameters for read and write operations.
    * Example:
    * For loading or exporting data from Teradata,
    * set "TYPE" parameter as "FASTLOAD" or "FASTEXPORT" (optional)
    *
    * @param dataset      Name of the data set
    * @param anyRDD       The RDD[T] to write
    * @param dataSetProps The given properties
    * @return RDD[T]
    */
  def write[T: universe.TypeTag](dataset: String, anyRDD: RDD[T], dataSetProps: Map[String, Any]): RDD[T] = {
    // TODO
    anyRDD
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet create for jdbc currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet drop for jdbc currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet truncate for jdbc currently not Supported")
  }

  /**
    * Save Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for JDBC Dataset")
  }

  /**
    * Clear Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for JDBC Dataset")
  }
}

/**
  * Custom Exception for JDBCDataSet initiation errors.
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class DataSetOperationException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}
