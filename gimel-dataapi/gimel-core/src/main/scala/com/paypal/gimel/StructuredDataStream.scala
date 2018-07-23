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

import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SparkSession}

import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf.{CatalogProviderConfigs, CatalogProviderConstants, GimelConstants}
import com.paypal.gimel.datastreamfactory.{GimelStructuredDataStream, StructuredStreamingResult}
import com.paypal.gimel.kafka.conf.{KafkaConstants}
import com.paypal.gimel.logger.Logger

object StructuredDataStreamType extends Enumeration {
  type SystemType = Value
  val KAFKA = Value
}

class StructuredDataStream(val sparkSession: SparkSession) {

  import com.paypal.gimel.common.utilities.DataSetUtils._

  val user: String = sys.env(GimelConstants.USER)
  val sparkContext: SparkContext = sparkSession.sparkContext
  val sparkAppName: String = sparkContext.getConf.get(GimelConstants.SPARK_APP_NAME)
  val appTag: String = getAppTag(sparkContext)
  val logger = Logger()
  val latestStructuredDataStreamReader: Option[GimelStructuredDataStream] = None

  import StructuredDataStreamUtils._

  def latestKafkaStructuredDataStreamReader: Option[com.paypal.gimel.kafka.StructuredDataStream] = {
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
  private def read(sourceType: DataStreamType.SystemType
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

    // Get catalog provider from run time hive context (1st Preference)
    // if not available - check user props (2nd Preference)
    // if not available - check Primary Provider of Catalog (Default)
    val formattedProps: Map[String, Any] = getProps(props) ++
      Map(CatalogProviderConfigs.CATALOG_PROVIDER ->
        sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER,
          CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER))
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

    logger.logApiAccess(sparkContext.getConf.getAppId
      , sparkContext.getConf.get("spark.app.name")
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeStream
      , getYarnClusterName()
      , user
      , appTag.replaceAllLiterally("/", "_")
      , MethodName
      , newProps("resolvedHiveTable").toString
      , "KAFKA"
      , ""
      , propsToLog)
    this.read(DataStreamType.KAFKA, dataSet, newProps)
  }

}

/**
  * Client API for initiating datastreams
  */

object StructuredDataStream {

  val defaultBatchInterval = 25

  import StructuredDataStreamUtils._

  /**
    * Client calls for a DataStream with SparkContext,
    * we internally create an HiveContext & provide DataStream
    *
    * @param sparkSession StreamingContext
    * @return DataStream
    */
  def apply(sparkSession: SparkSession): StructuredDataStream = {
    new StructuredDataStream(sparkSession)
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
    * @param sourceType Type of System. Example - KAFKA
    * @return PCatalogDataStream
    */

  def getStructuredDataStream(sparkSession: SparkSession
                    , sourceType: DataStreamType.SystemType): GimelStructuredDataStream = {
    sourceType match {
      case DataStreamType.KAFKA =>
        new com.paypal.gimel.kafka.StructuredDataStream(sparkSession)
    }
  }

  /**
    * Gets the last user Kafka KafkaDataStream reader (if already use), else Returns None
    *
    * @param dataStream DataStream
    * @return Option[KafkaDataStream]
    */

  def getLatestKafkaStructuredDataStreamReader(dataStream: StructuredDataStream)
  : Option[com.paypal.gimel.kafka.StructuredDataStream] = {
    val kafkaReader = Try {
      dataStream.latestStructuredDataStreamReader.get.asInstanceOf[com.paypal.gimel.kafka.StructuredDataStream]
    }
    kafkaReader match {
      case Success(x) =>
        Some(x)
      case _ =>
        None
    }
  }

}
