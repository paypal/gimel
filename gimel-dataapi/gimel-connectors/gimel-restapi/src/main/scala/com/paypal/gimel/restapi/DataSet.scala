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

package com.paypal.gimel.restapi

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.restapi.conf.{RestApiClientConfiguration, RestApiConstants}
import com.paypal.gimel.restapi.reader.RestApiConsumer
import com.paypal.gimel.restapi.writer.RestApiProducer

/**
  * Concrete Implementation for RestAPI DataSet
  *
  * @param sparkSession : SparkSession
  */

class DataSet(sparkSession: SparkSession) extends GimelDataSet(sparkSession: SparkSession) {

  // GET LOGGER
  val logger = Logger()
  logger.info(s"Initiated --> ${this.getClass.getName}")

  /**
    * Fetches all properties from spark session that start with - gimel.restapi.
    *
    * @param sparkSession Spark Session
    * @param props        Additional Properties
    * @return Properties
    */
  private def fuseWithPropsFromSparkSession(sparkSession: SparkSession
                                            , props: Map[String, Any] = Map()): Map[String, Any] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val propsFromSession = sparkSession.conf.getAll.filter(x => x._1.startsWith(RestApiConstants.restApiPatternString))
    logger.info("Props from Spark Session -->")
    propsFromSession.foreach(println)
    logger.info("Props already passed -->")
    props.foreach(println)
    props ++ propsFromSession
  }

  /** Read Implementation for RestAPI DataSet
    *
    * @param dataset      Name of the PCatalog Data Set
    * @param datasetProps props is the way to set various additional parameters for read and write operations in DataSet class
    * @return DataFrame
    */
  override def read(dataset: String, datasetProps: Map[String, Any]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    if (datasetProps.isEmpty) {
      throw new DataSetException("Props Map Cannot be emtpy for Read.")
    }
    // This logic supports the Sql parle, where users would use pattern - "set k=v"
    val allProps = fuseWithPropsFromSparkSession(sparkSession, datasetProps)
    val conf = new RestApiClientConfiguration(allProps)
    val data = RestApiConsumer.consume(sparkSession, conf)
    data
  }

  /** Write Implementation for RestAPI DataSet
    *
    * @param dataset      Name of the PCatalog Data Set
    * @param dataFrame    The DataFrame to write to target
    * @param datasetProps props is the way to set various additional parameters for read and write operations in DataSet class
    * @return DataFrame
    */

  override def write(dataset: String, dataFrame: DataFrame, datasetProps: Map[String, Any]): DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    if (datasetProps.isEmpty) {
      throw new DataSetException("Props Map Cannot be emtpy for  Write.")
    }
    // This logic supports the Sql parle, where users would use pattern - "set k=v"
    val allProps = fuseWithPropsFromSparkSession(sparkSession, datasetProps)
    val conf = new RestApiClientConfiguration(allProps)
    RestApiProducer.produce(conf, dataFrame)
    dataFrame
  }

  // Add Additional Supported types to this list as and when we support other Types of RDD
  // Example to start supporting RDD[String], add to List <  typeOf[Seq[Map[String, String]]].toString)  >
  override val supportedTypesOfRDD: List[String] = List[String](typeOf[String].toString)

  /**
    * Function writes a given dataframe to the actual Target System (Example Hive : DB.Table | HBASE namespace.Table)
    *
    * @param dataset      Name of the PCatalog Data Set
    * @param rdd          The RDD[T] to write into Target
    *                     Note the RDD has to be typeCast to supported types by the inheriting DataSet Operators
    *                     instance#1 : ElasticSearchDataSet may support just RDD[Seq(Map[String, String])], so Elastic Search must implement supported Type checking
    *                     instance#2 : Kafka, HDFS, HBASE - Until they support an RDD operation for Any Type T : They throw Unsupporter Operation Exception & Educate Users Clearly !
    * @param datasetProps props is the way to set various additional parameters for read and write operations in DataSet class
    * @return RDD[T]
    */
  def write[T: TypeTag](dataset: String, rdd: RDD[T], datasetProps: Map[String, Any]): RDD[T] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    if (!supportedTypesOfRDD.contains(typeOf[T].toString)) {
      throw new UnsupportedOperationException(s"""Invalid RDD Type. Supported Types : ${supportedTypesOfRDD.mkString(" | ")}""")
    } else {
      if (datasetProps.isEmpty) {
        throw new DataSetException("Props Map Cannot be emtpy for Write.")
      }
      // This logic supports the Sql parle, where users would use pattern - "set k=v"
      val allProps = fuseWithPropsFromSparkSession(sparkSession, datasetProps)
      val conf = new RestApiClientConfiguration(allProps)
      val rdd1: RDD[String] = rdd.asInstanceOf[RDD[String]]
      RestApiProducer.produce(conf, rdd1)
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
    throw new Exception(s"DataSet create for kafka currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def drop(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet drop for kafka currently not Supported")
  }

  /**
    *
    * @param dataset   Name of the UDC Data Set
    * @param dataSetProps
    * * @return Boolean
    */
  override def truncate(dataset: String, dataSetProps: Map[String, Any]): Unit = {
    throw new Exception(s"DataSet truncate for kafka currently not Supported")
  }

  /**
    * Save Checkpoint
    */
  override def clearCheckPoint(): Unit = {
    logger.info(s"Clear check Point functionality is not available for Rest API Dataset")
  }

  /**
    * Clear Checkpoint
    */
  override  def saveCheckPoint(): Unit = {
    logger.info(s"Save check Point functionality is not available for Rest API Dataset")
  }
}

/**
  * Custom Exception for RestAPIDataSet initiation errors
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
