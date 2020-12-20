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

package com.paypal.gimel.cassandra

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.cassandra.conf.CassandraClientConfiguration
import com.paypal.gimel.cassandra.reader.CassandraReader
import com.paypal.gimel.cassandra.writer.CassandraWriter
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.logger.Logger

/**
  * Concrete Implementation for Cassandra DataSet.
  *
  * @param sparkSession: SparkSession
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
    if (datasetProps.isEmpty) {
      throw new DataSetException("Props Map Cannot be emtpy for KafkaDataSet Read.")
    }
    val conf = new CassandraClientConfiguration(datasetProps)
    val data = CassandraReader.readTable(sparkSession, conf)
    data
  }

  /** Write Implementation for Cassandra DataSet.
    *
    * @param dataset      Name of the UDC Data Set.
    * @param dataFrame    The DataFrame to write to target.
    * @param datasetProps Additional parameters for read and write operations in DataSet class.
    * @return DataFrame
    */

  override def write(dataset: String, dataFrame: DataFrame,
                     datasetProps: Map[String, Any]): DataFrame = {

    if (datasetProps.isEmpty) {
      throw new DataSetException("Props Map Cannot be emtpy for KafkaDataSet Write.")
    }
    val conf = new CassandraClientConfiguration(datasetProps)
    CassandraWriter.writeToTable(sparkSession, conf, dataFrame)
    dataFrame
  }

  // Add Additional Supported types to this list as and when we support other Types of RDD
  // Example to support RDD[String], add to List < `typeOf[Seq[Map[String, String]]].toString)`>
  override val supportedTypesOfRDD: List[String] = List[String]()

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
    * @param dataset Name of the UDC Data Set.
    * @param rdd     The RDD[T] to write into Target.
    * @param datasetProps
    * @return RDD[T]
    */
  def write[T: TypeTag](dataset: String, rdd: RDD[T], datasetProps: Map[String, Any]): RDD[T] = {
    val msg = s"""Invalid RDD Type. Supported Types : ${supportedTypesOfRDD.mkString(" | ")}"""
    throw new UnsupportedOperationException(msg)
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet create for cassandra currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet drop for cassandra currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet truncate for cassandra currently not Supported")
  }

  /**
    * Save Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for Cassandra Dataset")
  }

  /**
    * Clear Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for Cassandra Dataset")
  }
}

/**
  * Custom Exception for KafkaDataSet initiation errors
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
