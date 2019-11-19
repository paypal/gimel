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

package com.paypal.gimel

import scala.collection.immutable.Map
import scala.language.implicitConversions
import scala.util.{Success, Try}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf.{CatalogProviderConfigs, CatalogProviderConstants, GimelConstants}
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.datastreamfactory.{GimelDataStream, StreamingResult}
import com.paypal.gimel.kafka.conf.{KafkaConfigs, KafkaConstants}
import com.paypal.gimel.logger.Logger

object DataStreamType extends Enumeration {
  type SystemType = Value
  val KAFKA = Value
}

class DataStream(val streamingContext: StreamingContext) {

  import com.paypal.gimel.common.utilities.DataSetUtils._

  val user: String = sys.env(GimelConstants.USER)
  val sparkAppName: String = streamingContext.sparkContext.getConf.get(GimelConstants.SPARK_APP_NAME)
  val appTag: String = getAppTag(streamingContext.sparkContext)
  val sparkContext: SparkContext = streamingContext.sparkContext
  val logger = Logger()
  logger.setSparkVersion(streamingContext.sparkContext.version)
  val latestDataStreamReader: Option[GimelDataStream] = None
  var datasetSystemType: String = "KAFKA"
  var additionalPropsToLog = scala.collection.mutable.Map[String, String]()

  // get gimel timer object
  val gimelTimer = Timer()

  import DataStreamUtils._

  def latestKafkaDataStreamReader: Option[com.paypal.gimel.kafka.DataStream] = {
    getLatestKafkaDataStreamReader(this)
  }

  /**
    * Provides DStream for a given configuration
    *
    * @param sourceType DataStreamType.Type
    * @param sourceName Kafka Topic Name
    * @param props      Map of K->V kafka Properties
    * @return StreamingResult
    */
  private def read(sourceType: DataStreamType.SystemType
                   , sourceName: String, props: Any): StreamingResult = {
    val propsMap: Map[String, Any] = getProps(props)
    val dataStream = DataStreamUtils.getDataStream(streamingContext, sourceType)
    dataStream.read(sourceName, propsMap)
  }

  /**
    * Provides DStream for a given configuration
    *
    * @param dataSet Kafka Topic Name
    * @param props   Map of K->V kafka Properties
    * @return StreamingResult
    */
  def read(dataSet: String, props: Any = Map[String, Any]()): StreamingResult = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    // get start time
    val startTime = gimelTimer.start.get

    try {

      // Get catalog provider from run time hive context (1st Preference)
      // if not available - check user props (2nd Preference)
      // if not available - check Primary Provider of Catalog (Default)
      val formattedProps: Map[String, Any] =
      Map(CatalogProviderConfigs.CATALOG_PROVIDER -> CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER,
        GimelConstants.SPARK_APP_ID -> streamingContext.sparkContext.getConf.get(GimelConstants.SPARK_APP_ID),
        GimelConstants.SPARK_APP_NAME -> streamingContext.sparkContext.getConf.get(GimelConstants.SPARK_APP_NAME),
        GimelConstants.APP_TAG -> appTag) ++
        getProps(props)
      val dataSetProperties: DataSetProperties =
        CatalogProvider.getDataSetProperties(dataSet, formattedProps)
      //    dataSetProperties.
      //    val (systemType, hiveTableProps) = getSystemType(dataSet)
      //    val systemType = getSystemType1(dataSetProperties)
      val newProps: Map[String, Any] = getProps(props) ++ Map(
        GimelConstants.DATASET_PROPS -> dataSetProperties
        , GimelConstants.DATASET -> dataSet
        , GimelConstants.RESOLVED_HIVE_TABLE -> resolveDataSetName(dataSet)
        , GimelConstants.APP_TAG -> appTag)

      // Why are we doing this? Elastic Search Cannot Accept "." in keys
      val dataSetProps = dataSetProperties.props.map { case (k, v) =>
        k.replaceAllLiterally(".", "~") -> v
      }

      val propsToLog = scala.collection.mutable.Map[String, String]()
      dataSetProps.foreach(x => propsToLog.put(x._1, x._2))
      // additionalPropsToLog = propsToLog

      val data = this.read(DataStreamType.KAFKA, dataSet, newProps)


      // update log variables to push logs
      val endTime = gimelTimer.endTime.get
      val executionTime: Double = gimelTimer.endWithMillSecRunTime

      // post audit logs to KAFKA
      logger.logApiAccess(streamingContext.sparkContext.getConf.getAppId
        , streamingContext.sparkContext.getConf.get("spark.app.name")
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeStream
        , getYarnClusterName()
        , user
        , appTag.replaceAllLiterally("/", "_")
        , MethodName
        , dataSet
        , datasetSystemType
        , ""
        , additionalPropsToLog
        , GimelConstants.SUCCESS
        , GimelConstants.EMPTY_STRING
        , GimelConstants.EMPTY_STRING
        , startTime
        , endTime
        , executionTime
      )

      data
    }
    catch {
      case e: Throwable =>

        logger.info(s"Pushing to logs:  Error Description\n dataset=${dataSet}\n method=${MethodName}\n Error: ${e.printStackTrace()}")

        // update log variables to push logs
        val endTime = System.currentTimeMillis()
        val executionTime = endTime - startTime

        // post audit logs to KAFKA
        logger.logApiAccess(streamingContext.sparkContext.getConf.getAppId
          , streamingContext.sparkContext.getConf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeStream
          , getYarnClusterName()
          , user
          , appTag.replaceAllLiterally("/", "_")
          , MethodName
          , dataSet
          , datasetSystemType
          , ""
          , additionalPropsToLog
          , GimelConstants.FAILURE
          , e.toString + "\n" + e.getStackTraceString
          , GimelConstants.UNKNOWN_STRING
          , startTime
          , endTime
          , executionTime
        )


        // throw error to console
        logger.throwError(e.toString)

        val msg = s"Error in DataSet ${MethodName} Operation: ${e.printStackTrace()}"
        throw new DataSetOperationException(msg, e)
    }

  }

}

/**
  * Client API for initiating datastreams
  */

object DataStream {

  val defaultBatchInterval = 25

  import DataStreamUtils._

  /**
    * Client calls for a DataStream with SparkContext
    * , we internally create an HiveContext & provide DataStream
    *
    * @param sparkContext SparkContext
    * @return DataStream
    */
  def apply(sparkContext: SparkContext): DataStream = {
    // todo ADD LOGGING .... WARN USER of default value or pass a specific one explicitly
    getOrCreateLogger(sparkContext).warning("Initiating New Spark Context. " +
      "Please provide HiveContext if you already have One.")
    val allConfs = sparkContext.getConf.getAll.toMap
    val batchWindowSec = allConfs.getOrElse(KafkaConfigs.defaultBatchInterval
      , defaultBatchInterval.toString).toInt
    val ssc = new StreamingContext(sparkContext, Seconds(batchWindowSec))
    this (ssc)
  }

  /**
    * Client calls for a DataStream with already available HiveContext
    * , we provide a DataStream API with the same HiveConext
    *
    * @param hiveContext HiveContext
    * @return DataStream
    */
  def apply(hiveContext: HiveContext): DataStream = {
    getOrCreateLogger(hiveContext.sparkContext).warning("Initiating New Spark Context" +
      ". Please provide HiveContext if you already have One.")
    this (hiveContext.sparkContext)
  }

  /**
    * Client calls for a DataStream without any context (spark or hive)
    * , we provide a DataStream API with the same HiveConext
    *
    * @return DataStream
    */
  def apply(): DataStream = {
    val sparkConf = new SparkConf().setAppName(sys.env(GimelConstants.USER) + "PCataLog-DataSet")
    val sc = new SparkContext(sparkConf)
    getOrCreateLogger(sc).warning("Initiating New Spark Context" +
      ". Please provide HiveContext if you already have One.")
    this (sc)
  }

  /**
    * Client calls for a DataStream with already available SQLContext
    * , we provide a DataStream API with the equivalent HiveConext
    *
    * @param sqlContext SQLContext
    * @return DataStream
    */
  def apply(sqlContext: SQLContext): DataStream = {
    getOrCreateLogger(sqlContext.sparkContext).warning("Initiating New Spark Context. " +
      "Please provide HiveContext if you already have One.")
    this (sqlContext.sparkContext)
  }

  /**
    * Client calls for a DataStream with SparkContext,
    * we internally create an HiveContext & provide DataStream
    *
    * @param streamingContext StreamingContext
    * @return DataStream
    */
  def apply(streamingContext: StreamingContext): DataStream = {
    new DataStream(streamingContext)
  }

}

/**
  * Custom Exception for DataStream initiation errors
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class DataStreamInitializationException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}


/**
  * Private Functionalities required for DataStream Initiation Operations
  * Do Not Expose to Client
  */

private object DataStreamUtils {


  /**
    * Convenience Method to Get or Create Logger
    *
    * @param sparkContext SparkContext
    * @return Logger
    */
  def getOrCreateLogger(sparkContext: SparkContext): Logger = {
    val user = sys.env(GimelConstants.USER)
    val sparkAppName = sparkContext.getConf.get(GimelConstants.SPARK_APP_NAME)
    val appTag = s"${user}-${sparkAppName}"
    val logger = Logger(appTag)
    logger
  }

  /**
    * provides an appropriate DataStream
    *
    * @param sparkStreamingContext
    * @param sourceType Type of System. Example - KAFKA
    * @return DataStream
    */

  def getDataStream(sparkStreamingContext: StreamingContext
                    , sourceType: DataStreamType.SystemType): GimelDataStream = {
    sourceType match {
      case DataStreamType.KAFKA =>
        new com.paypal.gimel.kafka.DataStream(sparkStreamingContext)
    }
  }

  /**
    * Gets the last user Kafka KafkaDataStream reader (if already use), else Returns None
    *
    * @param dataStream DataStream
    * @return Option[KafkaDataStream]
    */

  def getLatestKafkaDataStreamReader(dataStream: DataStream)
  : Option[com.paypal.gimel.kafka.DataStream] = {
    val kafkaReader = Try {
      dataStream.latestDataStreamReader.get.asInstanceOf[com.paypal.gimel.kafka.DataStream]
    }
    kafkaReader match {
      case Success(x) =>
        Some(x)
      case _ =>
        None
    }
  }

}
