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

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.DataStreamWriter

import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf.{CatalogProviderConfigs, CatalogProviderConstants, GimelConstants}
import com.paypal.gimel.common.gimelserde.GimelSerdeUtils
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.datastreamfactory.{GimelDataStream2, StructuredStreamingResult}
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs
import com.paypal.gimel.kafka2.conf.KafkaConfigs
import com.paypal.gimel.kafka2.conf.KafkaConstants
import com.paypal.gimel.logger.Logger

object StructuredDataStreamType extends Enumeration {
  type SystemType = Value
  val KAFKA, HBASE, ES, JDBC, CONSOLE = Value
}

class DataStream2(val sparkSession: SparkSession) {

  import com.paypal.gimel.common.utilities.DataSetUtils._

  val user: String = sys.env(GimelConstants.USER)
  val sparkContext: SparkContext = sparkSession.sparkContext
  val sparkAppName: String = sparkContext.getConf.get(GimelConstants.SPARK_APP_NAME)
  val appTag: String = getAppTag(sparkContext)
  val logger = Logger()
  logger.setSparkVersion(sparkSession.version)
  val latestStructuredDataStreamReader: Option[GimelDataStream2] = None
  var datasetSystemType: String = "KAFKA"
  var additionalPropsToLog = scala.collection.mutable.Map[String, String]()

  // get gimel timer object
  val gimelTimer = Timer()

  import StructuredDataStreamUtils._

  def latestKafkaStructuredDataStreamReader: Option[com.paypal.gimel.kafka2.DataStream] = {
    getLatestKafkaStructuredDataStreamReader(this)
  }

  /**
    * Provides DStream for a given configuration
    *
    * @param sourceType DataStreamType.Type
    * @param sourceName Kafka Topic Name
    * @param props      Map of K->V kafka Properties
    * @return StreamingResult
    */
  private def read(sourceType: StructuredDataStreamType.SystemType
                   , sourceName: String, props: Any): StructuredStreamingResult = {
    val propsMap: Map[String, Any] = getProps(props)

    val dataStream = StructuredDataStreamUtils.getStructuredDataStream(sparkSession, sourceType)
    dataStream.read(sourceName, propsMap)
  }

  /**
    * Provides DStream for a given configuration
    *
    * @param dataSet Kafka Topic Name
    * @param props   Map of K->V kafka Properties
    * @return StreamingResult
    */
  def read(dataSet: String, props: Any = Map[String, Any]()): StructuredStreamingResult = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    // get start time
    val startTime = gimelTimer.start.get

    try {

      // Set gimel log level and flag to audit logs to kafka
      setGimelLogLevel(sparkSession, logger)

      // Get catalog provider from run time hive context (1st Preference)
      // if not available - check user props (2nd Preference)
      // if not available - check Primary Provider of Catalog (Default)
      val formattedProps: Map[String, Any] = getProps(props) ++
        Map(CatalogProviderConfigs.CATALOG_PROVIDER ->
          sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER,
            CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER),
          GimelConstants.SPARK_APP_ID -> sparkSession.conf.get(GimelConstants.SPARK_APP_ID),
          GimelConstants.SPARK_APP_NAME -> sparkSession.conf.get(GimelConstants.SPARK_APP_NAME),
          GimelConstants.APP_TAG -> appTag)
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

      val streamingResult = this.read(StructuredDataStreamType.KAFKA, dataSet, newProps)

      // To deserialize the resultant Dataframe if serializer class is given (Using Class Loader)
      val deserializerClassName = newProps.getOrElse(GimelConstants.GIMEL_DESERIALIZER_CLASS,
        dataSetProperties.props.getOrElse(GimelConstants.GIMEL_DESERIALIZER_CLASS, "")).toString

      val deserializedStreamingResult = if (deserializerClassName.isEmpty) {
        streamingResult
      } else {
        val deserializerObj = GimelSerdeUtils.getDeserializerObject(deserializerClassName)
        val df = streamingResult.df
        val deserializedDf = deserializerObj.deserialize(df, dataSetProperties.props ++ newProps)
        StructuredStreamingResult(deserializedDf, streamingResult.saveCheckPoint, streamingResult.clearCheckPoint)
      }

      // update log variables to push logs
      val endTime = gimelTimer.endTime.get
      val executionTime: Double = gimelTimer.endWithMillSecRunTime


      // post audit logs to KAFKA
      logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkContext.getConf.get("spark.app.name")
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

      deserializedStreamingResult
    }
    catch {
      case e: Throwable =>

        logger.info(s"Pushing to logs:  Error Description\n dataset=${dataSet}\n method=${MethodName}\n Error: ${e.printStackTrace()}")

        // update log variables to push logs
        val endTime = System.currentTimeMillis()
        val executionTime = endTime - startTime

        // post audit logs to KAFKA
        logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkContext.getConf.get("spark.app.name")
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

  /**
    * Writes Stream to a target
    *
    * @param targetType DataStreamType.Type
    * @param sourceName Kafka Topic Name
    * @param props      Map of K->V kafka Properties
    * @return StreamingResult
    */
  private def write(targetType: StructuredDataStreamType.SystemType,
                    sourceName: String, dataFrame: DataFrame, props: Any): DataStreamWriter[Row] = {
    val propsMap: Map[String, Any] = getProps(props)

    val dataStream = StructuredDataStreamUtils.getStructuredDataStream(sparkSession, targetType)
    dataStream.write(sourceName, dataFrame, propsMap)
  }

  /**
    * Provides DStream for a given configuration
    *
    * @param dataSet Kafka Topic Name
    * @param props   Map of K->V kafka Properties
    * @return StreamingResult
    */
  def write(dataSet: String, dataFrame: DataFrame, props: Any = Map[String, Any]()): DataStreamWriter[Row] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    // get start time
    val startTime = gimelTimer.start.get

    try {

      // Set gimel log level and flag to audit logs to kafka
      setGimelLogLevel(sparkSession, logger)

      // Get catalog provider from run time hive context (1st Preference)
      // if not available - check user props (2nd Preference)
      // if not available - check Primary Provider of Catalog (Default)
      val formattedProps: Map[String, Any] = getProps(props) ++
        Map(CatalogProviderConfigs.CATALOG_PROVIDER ->
          sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER,
            CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER),
          GimelConstants.SPARK_APP_ID -> sparkSession.conf.get(GimelConstants.SPARK_APP_ID),
          GimelConstants.SPARK_APP_NAME -> sparkSession.conf.get(GimelConstants.SPARK_APP_NAME),
          GimelConstants.APP_TAG -> appTag)
      val dataSetProperties: DataSetProperties =
        CatalogProvider.getDataSetProperties(dataSet, formattedProps)
      //    dataSetProperties.
      //    val (systemType, hiveTableProps) = getSystemType(dataSet)
      //    val systemType = getSystemType1(dataSetProperties)

      val checkpointLocation = getStructuredStreamingCheckpointLocation(sparkContext)

      // Here fully qualified method name is present as it was conflicting with getSystemType in DataSetUtils
      val systemType = com.paypal.gimel.StructuredDataStreamUtils.getSystemType(dataSetProperties)
      val newProps: Map[String, Any] = getProps(props) ++ Map(
        GimelConstants.DATASET_PROPS -> dataSetProperties
        , GimelConstants.DATASET -> dataSet
        , GimelConstants.RESOLVED_HIVE_TABLE -> resolveDataSetName(dataSet)
        , GimelConstants.APP_TAG -> appTag
        , GimelConstants.GIMEL_STREAMING_CHECKPOINT_LOCATION -> checkpointLocation)

      // Why are we doing this? Elastic Search Cannot Accept "." in keys
      val dataSetProps = dataSetProperties.props.map { case (k, v) =>
        k.replaceAllLiterally(".", "~") -> v
      }

      val propsToLog = scala.collection.mutable.Map[String, String]()
      dataSetProps.foreach(x => propsToLog.put(x._1, x._2))
      datasetSystemType = systemType.toString

      // To serialize the resultant Dataframe if serializer class is given (Using Class Loader)
      val serializerClassName = newProps.getOrElse(GimelConstants.GIMEL_SERIALIZER_CLASS,
        dataSetProperties.props.getOrElse(GimelConstants.GIMEL_SERIALIZER_CLASS, "")).toString

      val serializedData = if (serializerClassName.isEmpty) {
        dataFrame
      } else {
        val serializerObj = GimelSerdeUtils.getSerializerObject(serializerClassName)
        serializerObj.serialize(dataFrame, dataSetProperties.props ++ newProps)
      }

      val dataStreamWriter = this.write(systemType, dataSet, serializedData, newProps)

      // update log variables to push logs
      val endTime = gimelTimer.endTime.get
      val executionTime: Double = gimelTimer.endWithMillSecRunTime

      // post audit logs to KAFKA
      logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkContext.getConf.get("spark.app.name")
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
      dataStreamWriter
    } catch {
      case e: Throwable =>

        logger.info(s"Pushing to logs:  Error Description\n dataset=${dataSet}\n method=${MethodName}\n Error: ${e.printStackTrace()}")

        // update log variables to push logs
        val endTime = System.currentTimeMillis()
        val executionTime = endTime - startTime

        // post audit logs to KAFKA
        logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkContext.getConf.get("spark.app.name")
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

object DataStream2 {

  /**
    * Client calls for a DataStream with SparkContext,
    * we internally create an HiveContext & provide DataStream
    *
    * @param sparkSession StreamingContext
    * @return DataStream
    */
  def apply(sparkSession: SparkSession): DataStream2 = {
    new DataStream2(sparkSession)
  }

}


/**
  * Private Functionalities required for DataStream Initiation Operations
  * Do Not Expose to Client
  */

private object StructuredDataStreamUtils {


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
    * provides an appropriate PCatalogDataStream
    *
    * @param sparkSession
    * @param systemType Type of System. Example - KAFKA
    * @return PCatalogDataStream
    */

  def getStructuredDataStream(sparkSession: SparkSession, systemType: StructuredDataStreamType.SystemType): GimelDataStream2 = {
    systemType match {
      case StructuredDataStreamType.KAFKA =>
        new com.paypal.gimel.kafka2.DataStream(sparkSession)
    }
  }

  /**
    * Gets the last user Kafka KafkaDataStream reader (if already use), else Returns None
    *
    * @param dataStream DataStream
    * @return Option[KafkaDataStream]
    */

  def getLatestKafkaStructuredDataStreamReader(dataStream: DataStream2)
  : Option[com.paypal.gimel.kafka2.DataStream] = {
    val kafkaReader = Try {
      dataStream.latestStructuredDataStreamReader.get.asInstanceOf[com.paypal.gimel.kafka2.DataStream]
    }
    kafkaReader match {
      case Success(x) =>
        Some(x)
      case _ =>
        None
    }
  }

  /**
    * Fetch the Type of DataStreamType based on the DataSetProperties that is Supplied
    *
    * @param dataSetProperties DataSetProperties
    * @return DataStreamType
    */

  def getSystemType(dataSetProperties: DataSetProperties): (StructuredDataStreamType.Value) = {

    val storageHandler = dataSetProperties.props.getOrElse(GimelConstants.STORAGE_HANDLER, GimelConstants.NONE_STRING)
    val storageType = dataSetProperties.datasetType

    val systemType = storageHandler match {
      case ElasticSearchConfigs.esStorageHandler =>
        StructuredDataStreamType.ES
      case KafkaConfigs.kafkaStorageHandler =>
        StructuredDataStreamType.KAFKA
      case _ =>
        storageType.toUpperCase() match {
          case "KAFKA" =>
            StructuredDataStreamType.KAFKA
          case "ELASTIC_SEARCH" =>
            StructuredDataStreamType.ES
          case _ =>
            val msg = s"Unsupported Storage Format for Gimel Streaming --> " + storageType.toUpperCase()
            throw new DataSetOperationException(msg)
        }
    }
    systemType
  }
}
