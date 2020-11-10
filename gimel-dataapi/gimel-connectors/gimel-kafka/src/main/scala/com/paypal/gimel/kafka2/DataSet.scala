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

package com.paypal.gimel.kafka2

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.OffsetRange

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.kafka2.conf.{KafkaClientConfiguration, KafkaConfigs}
import com.paypal.gimel.kafka2.reader.KafkaBatchConsumer
import com.paypal.gimel.kafka2.utilities.ImplicitZKCheckPointers._
import com.paypal.gimel.kafka2.utilities.ZooKeeperHostAndNodes
import com.paypal.gimel.kafka2.writer.KafkaBatchProducer
import com.paypal.gimel.logger.Logger

/**
  * Concrete Implementation for Kafka DataSet
  *
  * @param sparkSession : SparkSession
  */
class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  var readTillOffsetRange: Option[Array[OffsetRange]] = None
  var alreadyCheckPointed = false
  // FIXME What happens if two users call read and write at the same time? Data race over conf?
  var conf: KafkaClientConfiguration = _

  /**
    * Saves Currently Read Offsets to Zookeeper
    */
  override def saveCheckPoint(): Unit = {
    if (alreadyCheckPointed) {
      logger.warning("Already Check-Pointed, Consume Again to Checkpoint !")
    } else {
      val zkNode = conf.zkCheckPoints
      val zkHost = conf.zkHostAndPort
      val zk = ZooKeeperHostAndNodes(zkHost, zkNode)
      (zk, readTillOffsetRange.get).saveZkCheckPoint
      alreadyCheckPointed = true
      logger.info(s"Check-Point --> ${readTillOffsetRange.get.mkString("|")} | Success @ -> ${zk} ")
    }
  }

  /**
    * Completely Clear the CheckPointed Offsets, leading to Read from Earliest offsets from Kafka
    */
  override def clearCheckPoint(): Unit = {
    val zkNode = conf.zkCheckPoints
    val zkHost = conf.zkHostAndPort
    val zk = ZooKeeperHostAndNodes(zkHost, zkNode)
    zk.deleteZkCheckPoint()
  }

  /** Read Implementation for Kafka DataSet
    *
    * @param dataset Name of the PCatalog Data Set
    * @param datasetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to read kafka from-to a certain offset range : One can set something like below -
    *                val props = Map("fromOffset" -> 10, "toOffset" -> 20)
    *                val data = Dataset(sc).read("flights.topic", props)
    * @return DataFrame
    */
  override def read(dataset: String, datasetProps: Map[String, Any]): DataFrame = {

    if (datasetProps.isEmpty) {
      throw new DataSetException("Props Map Cannot be empty for KafkaDataSet Read.")
    }
    logger.info("New Kafka API Version 2 is called")

    val actualProps: DataSetProperties = datasetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val tableProps: Map[String, String] = actualProps.props

    conf = new KafkaClientConfiguration(datasetProps)

    if (tableProps.get(KafkaConfigs.kafkaConsumerClearCheckpointKey).contains("true")) {
      logger.info("Clearing Checkpoint")
      this.clearCheckPoint()
    }

    val (data, toOffset) = KafkaBatchConsumer.consumeFromKakfa(sparkSession, conf)

    alreadyCheckPointed = false
    readTillOffsetRange = Some(toOffset)
    data
  }

  /** Write Implementation for Kafka DataSet
    *
    * @param dataset   Name of the PCatalog Data Set
    * @param dataFrame The DataFrame to write to target
    * @param datasetProps
    *                  props is the way to set various additional parameters for read and write operations in DataSet class
    *                  Example Usecase : to write kafka with a specific parallelism : One can set something like below -
    *                  val props = Map("parallelsPerPartition" -> 10)
    *                  Dataset(sc).write(clientDataFrame, props)
    * @return DataFrame
    */

  override def write(dataset: String, dataFrame: DataFrame, datasetProps: Map[String, Any]): DataFrame = {

    if (datasetProps.isEmpty) {
      throw new DataSetException("Props Map Cannot be empty for KafkaDataSet Write.")
    }
    conf = new KafkaClientConfiguration(datasetProps)
    KafkaBatchProducer.produceToKafka(conf, dataFrame)
    dataFrame
  }

  /**
    *
    * @param dataset Name of the PCatalog Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def create(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet create for kafka currently not Supported")
  }

  /**
    *
    * @param dataset Name of the PCatalog Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet drop for kafka currently not Supported")
  }

  /**
    *
    * @param dataset Name of the PCatalog Data Set
    * @param dataSetProps
    *                * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet truncate for kafka currently not Supported")
  }


  // Add Additional Supported types to this list as and when we support other Types of RDD
  // Example to start supporting RDD[String], add to List <  typeOf[Seq[Map[String, String]]].toString)  >
  override val supportedTypesOfRDD: List[String] = List[String](typeOf[String].toString, typeOf[Array[Byte]].toString)

  /**
    * Function writes a given dataframe to the actual Target System (Example Hive : DB.Table | HBASE namespace.Table)
    *
    * @param dataset Name of the PCatalog Data Set
    * @param rdd     The RDD[T] to write into Target
    *                Note the RDD has to be typeCast to supported types by the inheriting DataSet Operators
    *                instance#1 : ElasticSearchDataSet may support just RDD[Seq(Map[String, String])], so Elastic Search must implement supported Type checking
    *                instance#2 : Kafka, HDFS, HBASE - Until they support an RDD operation for Any Type T : They throw Unsupporter Operation Exception & Educate Users Clearly !
    * @param datasetProps
    *                props is the way to set various additional parameters for read and write operations in DataSet class
    *                Example Usecase : to write kafka with a specific parallelism : One can set something like below -
    *                val props = Map("parallelsPerPartition" -> 10)
    *                Dataset(sc).write(clientDataFrame, props)
    * @return RDD[T]
    */
  def write[T: TypeTag](dataset: String, rdd: RDD[T], datasetProps: Map[String, Any]): RDD[T] = {
    throw new Exception(s"RDD Write is not supported")
    rdd
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
