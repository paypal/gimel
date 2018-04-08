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

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.util.Try

import org.apache.spark.{SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}

import com.paypal.gimel.common.catalog._
import com.paypal.gimel.common.conf._
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.jdbc.conf.JdbcConfigs
import com.paypal.gimel.kafka.conf.{KafkaConfigs, KafkaConstants}
import com.paypal.gimel.logger.Logger


object DataSetType extends Enumeration {
  type SystemType = Value
  val KAFKA, HBASE, HDFS, ES, HIVE, JDBC, CASSANDRA, AEROSPIKE, DRUID = Value
}

class DataSet(val sparkSession: SparkSession) {

  import com.paypal.gimel.common.utilities.DataSetUtils._

  val user = sys.env("USER")
  val sparkAppName = sparkSession.conf.get(GimelConstants.SPARK_APP_NAME)
  val clusterName = getYarnClusterName()
  val appTag = getAppTag(sparkSession.sparkContext)
  val logger = Logger(this.getClass.getName)
  val sparkContext: SparkContext = sparkSession.sparkContext
  val sqlContext: SQLContext = sparkSession.sqlContext
  var latestDataSetReader: Option[GimelDataSet] = None
  var latestDataSetWriter: Option[GimelDataSet] = None
  val currentTime = System.currentTimeMillis().toString
  val loadTag = appTag + "_" + currentTime
  sparkSession.sparkContext.setLogLevel("ERROR")

  import DataSetUtils._

  def latestKafkaDataSetReader: Option[com.paypal.gimel.kafka.DataSet] = {
    getLatestKafkaDataSetReader(this)
  }

  /**
    * Client API : for read
    *
    * @param dataSet DataSet Name | DB.TABLE | Example : default.temp
    * @param props   Additional Properties for the Reader of Dataset
    * @return DataFrame
    */

  def read(dataSet: String, props: Any = Map[String, Any]()): DataFrame = {
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
    val systemType = getSystemType(dataSetProperties)
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

    logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkAppName
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeBatch
      , clusterName
      , user
      , appTag.replaceAllLiterally("/", "_")
      , MethodName
      , newProps("resolvedHiveTable").toString
      , systemType.toString
      , ""
      , propsToLog
    )
    val data = this.read(systemType, dataSet, newProps)
    data
  }

  /**
    * Client API : Calls appropriate DataSet & its Write method
    *
    * @param dataSet   Example : flights.flights_log | flights | default:flights
    * @param dataFrame DataFrame
    * @param props     Additional Properties for the Reader of Dataset
    * @return DataFrame
    */
  def write(dataSet: String, dataFrame: DataFrame
            , props: Any = Map[String, Any]()): DataFrame = {
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
    val systemType = getSystemType(dataSetProperties)
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

    logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkAppName
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeBatch
      , clusterName
      , user
      , appTag.replaceAllLiterally("/", "_")
      , MethodName
      , newProps("resolvedHiveTable").toString
      , systemType.toString
      , ""
      , propsToLog)
    this.write(systemType, dataSet, dataFrame, newProps)
    dataFrame
  }

  /**
    * Client API : Calls appropriate DataSet & its Write method
    *
    * @param dataSet Example : flights.flights_log | flights | default:flights
    * @param anyRDD  RDD[T] UnSupported Types may fail at run-time.
    *                Please check Documentation of APIs carefully.
    * @param props   Additional Properties for the Reader of Dataset
    * @return
    */

  def write[T: TypeTag](dataSet: String, anyRDD: RDD[T], props: Any): RDD[T] = {
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
    val systemType = getSystemType(dataSetProperties)
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

    logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkAppName
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeBatch
      , clusterName
      , user
      , appTag.replaceAllLiterally("/", "_")
      , MethodName
      , newProps("resolvedHiveTable").toString
      , systemType.toString
      , ""
      , propsToLog
    )
    anyRDD
  }

  /**
    * Calls appropriate DataSet & its read method
    *
    * @param sourceType Example : KAFKA | ELASTIC_SEARCH | HDFS | HBASE  |JDBC
    * @param sourceName Example : flights.flights_log | flights | default:flights
    * @param props      Additional Properties for the Reader of Dataset
    * @return DataFrame
    */
  private def read(sourceType: DataSetType.SystemType
                   , sourceName: String
                   , props: Any): DataFrame = {
    val propsMap: Map[String, Any] = getProps(props)
    latestDataSetReader = Some(getDataSet(sparkSession, sourceType))
    latestDataSetReader.get.read(sourceName, propsMap)
  }

  /**
    * Calls appropriate DataSet & its Write method
    *
    * @param targetType Example : KAFKA | ELASTIC_SEARCH | HDFS | HBASE | JDBC
    * @param targetName Example : flights.flights_log | flights | default:flights
    * @param dataFrame  DataFrame
    * @param props      Additional Properties for the Reader of Dataset
    * @return DataFrame
    */
  private def write(targetType: DataSetType.SystemType
                    , targetName: String
                    , dataFrame: DataFrame
                    , props: Any): DataFrame = {
    val propsMap: Map[String, Any] = getProps(props)
    latestDataSetWriter = Some(getDataSet(sparkSession, targetType))
    latestDataSetWriter.get.write(targetName, dataFrame, propsMap)
  }

  /**
    * Calls appropriate DataSet & its Write method
    *
    * @param targetType Example : KAFKA | ELASTIC_SEARCH | HDFS | HBASE  | JDBC
    * @param targetName Example : flights.flights_log | flights | default:flights
    * @param anyRDD     RDD[T] UnSupported Types may fail at run-time.
    *                   Please check Documentation of APIs carefully.
    * @param props      Additional Properties for the Reader of Dataset
    * @return
    */
  private def write[T: TypeTag](targetType: DataSetType.SystemType
                                , targetName: String
                                , anyRDD: RDD[T]
                                , props: Any): RDD[T] = {
    val propsMap: Map[String, Any] = getProps(props)
    latestDataSetWriter = Some(getDataSet(sparkSession, targetType))
    latestDataSetWriter.map(_.write(targetName, anyRDD, propsMap)).orNull
  }

}

/**
  * Client API for initiating datasets
  */

object DataSet {

  import DataSetUtils._


  /**
    * Client calls for a DataSet with SparkSession
    *
    * @param sparkSession : SparkSession
    * @return DataSet
    */
  def apply(sparkSession: SparkSession): DataSet = {
    new DataSet(sparkSession)
  }

}

/**
  * Custom Exception for DataSet initiation errors
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class DataSetInitializationException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}


/**
  * Private Functionalities required for DataSet Initiation Operations
  * Do Not Expose to Client
  */

object DataSetUtils {

  import com.paypal.gimel.common.utilities.DataSetUtils._

  /**
    * Convenience Method to Get or Create Logger
    *
    * @param sparkSession SparkSession
    * @return Logger
    */
  def getOrCreateLogger(sparkSession: SparkSession): Logger = {
    val user = sys.env("USER")
    val sparkAppName = sparkSession.conf.get(GimelConstants.SPARK_APP_NAME)
    val appTag = s"$user-$sparkAppName"
    val logger = Logger(appTag)
    logger
  }

  /**
    * Fetch the Type of DataSetType based on the DataSetProperties that is Supplied
    *
    * @param dataSetProperties DataSetProperties
    * @return DataSetType
    */

  def getSystemType(dataSetProperties: DataSetProperties): (DataSetType.Value) = {

    val storageHandler = dataSetProperties.props.getOrElse(GimelConstants.STORAGE_HANDLER, GimelConstants.NONE_STRING)
    val storageType = dataSetProperties.datasetType

    val systemType = storageHandler match {
      case HbaseConfigs.hbaseStorageHandler =>
        DataSetType.HBASE
      case ElasticSearchConfigs.esStorageHandler =>
        DataSetType.ES
      case KafkaConfigs.kafkaStorageHandler =>
        DataSetType.KAFKA
      case JdbcConfigs.jdbcStorageHandler =>
        DataSetType.JDBC
      case _ =>
        storageType.toUpperCase() match {
          case "HBASE" =>
            DataSetType.HBASE
          case "KAFKA" =>
            DataSetType.KAFKA
          case "ELASTIC_SEARCH" =>
            DataSetType.ES
          case "JDBC" =>
            DataSetType.JDBC
          case "CASSANDRA" =>
            DataSetType.CASSANDRA
          case "AEROSPIKE" =>
            DataSetType.AEROSPIKE
          case "DRUID" =>
            DataSetType.DRUID
          case "HDFS" =>
            DataSetType.HDFS
          case _ =>
            DataSetType.HIVE
        }
    }
    systemType
  }

  /**
    * provides an appropriate PCatalogDataSet
    *
    * @param sparkSession : SparkSession
    * @param sourceType   Type of System. Example - HBASE , ES, KAFKA, HDFS, MYSQL
    * @return PCatalogDataSet
    */

  def getDataSet(sparkSession: SparkSession, sourceType: DataSetType.SystemType): GimelDataSet = {
    sourceType match {
      case DataSetType.KAFKA =>
        new com.paypal.gimel.kafka.DataSet(sparkSession)
      case DataSetType.HBASE =>
        new com.paypal.gimel.hbase.DataSet(sparkSession)
      case DataSetType.HDFS =>
        new com.paypal.gimel.hdfs.DataSet(sparkSession)
      case DataSetType.ES =>
        new com.paypal.gimel.elasticsearch.DataSet(sparkSession)
      case DataSetType.JDBC =>
        new com.paypal.gimel.jdbc.DataSet(sparkSession)
      case DataSetType.HIVE =>
        new com.paypal.gimel.hive.DataSet(sparkSession)
      case DataSetType.CASSANDRA =>
        new com.paypal.gimel.cassandra.DataSet(sparkSession)
      case DataSetType.AEROSPIKE =>
        new com.paypal.gimel.aerospike.DataSet(sparkSession)
      case DataSetType.HDFS =>
        new com.paypal.gimel.hdfs.DataSet(sparkSession)
      case DataSetType.DRUID =>
        new com.paypal.gimel.druid.DataSet(sparkSession)
    }
  }

  /**
    * Gets the last user Kafka DataSet reader (if already use), else Returns None
    *
    * @param dataSet DataSet
    * @return Option[KafkaDataSet]
    */

  def getLatestKafkaDataSetReader(dataSet: DataSet): Option[com.paypal.gimel.kafka.DataSet] = {
    Try {
      dataSet.latestDataSetReader.get.asInstanceOf[com.paypal.gimel.kafka.DataSet]
    }.toOption
  }
}
