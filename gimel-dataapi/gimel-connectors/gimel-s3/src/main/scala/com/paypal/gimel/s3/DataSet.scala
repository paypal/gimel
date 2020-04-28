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

package com.paypal.gimel.s3

import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.s3.conf.S3ClientConfiguration
import com.paypal.gimel.s3.reader.S3Reader
import com.paypal.gimel.s3.writer.S3Writer

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  /**
    * Read wrapper which will call the s3 reader implementation read method which does the actual reading from the S3 server
    *
    * @param dataset      - Name of the PCatalog Data Set
    * @param dataSetProps - props is the way to set various additional parameters for read and write operations in DataSet class
    * @return - a Dataframe consisting of the SFTP file records
    */
  override def read(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)
    val conf = new S3ClientConfiguration(dataSetProps)
    try {
      S3Reader.read(sparkSession, conf)
    } catch {
      case e: Throwable =>
        val msg = s"Error reading from S3 : ${e.getMessage}"
        throw new DataSetOperationException(msg, e)
    }
  }

  /**
    * Write wrapper method which will call the s3 writer implementation which does the actual writing of data from the dataframe to the S3 location
    *
    * @param dataset      - Name of the PCatalog Data Set
    * @param dataFrame    - The DataFrame to write to target
    * @param dataSetProps - props is the way to set various additional parameters for read and write operations in DataSet class
    * @return - the same dataframe is returned
    */
  override def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)
    val conf = new S3ClientConfiguration(dataSetProps)
    try {
      val dataframe = S3Writer.write(sparkSession, dataFrame, conf)
      dataframe
    } catch {
      case e: Throwable =>
        val msg = s"Error writing into S3 : ${e.getMessage}"
        throw new DataSetOperationException(msg, e)
    }
  }

  /**
    * Presently we don't support writing RDD
    * @param dataset - Name of the PCatalog Data Set
    * @param rdd - Incoming rdd to be written
    * @param datasetProps - props is the way to set various additional parameters for read and write operations in DataSet class
    * @tparam T - the typeof the data
    * @return - Same RDD is returned.
    */
  def write[T: TypeTag](dataset: String, rdd: RDD[T], datasetProps: Map[String, Any]): RDD[T] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)
    rdd
  }

  /**
    *
    * @param dataset   Name of the PCatalog Data Set
    * @param dataSetProps - props is the way to set various additional parameters for read and write operations in DataSet class
    * * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new UnsupportedOperationException(s"DataSet create for S3 currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the PCatalog Data Set
    * @param dataSetProps - props is the way to set various additional parameters for read and write operations in DataSet class
    * * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new UnsupportedOperationException(s"DataSet drop for S3 currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the PCatalog Data Set
    * @param dataSetProps - props is the way to set various additional parameters for read and write operations in DataSet class
    * * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new UnsupportedOperationException(s"DataSet truncate for S3 currently not Supported")
  }

  /**
    * Save Kafka Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for S3 Dataset")
  }

  /**
    * Clear Kafka Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for S3 Dataset")
  }



  /**
    * Custom Exception for S3 read write errors.
    *
    * @param message Message to Throw
    * @param cause   A Throwable Cause
    */
  private class DataSetOperationException(message: String, cause: Throwable)
    extends RuntimeException(message, cause) {

    def this(message: String) = this(message, null)
  }

}
