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

package com.paypal.gimel.common.utilities

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import scala.collection.immutable.Map
import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf.{CatalogProviderConfigs, CatalogProviderConstants, GimelConstants}
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.parser.utilities.QueryParserUtils

object DataSetType extends Enumeration {
  type SystemType = Value
  val KAFKA, HBASE, HDFS, ES, HIVE, JDBC, CASSANDRA, AEROSPIKE, DRUID, RESTAPI, SFTP, KAFKA2 = Value
}

object DataSetUtils {

  private val logger: Logger = Logger(this.getClass.getName)

  /**
    * Resolves the DatasetName by adding "default" as database if user passes just table name
    *
    * @param sourceName DB.Table or just Table
    * @return DB.Table
    */
  def resolveDataSetName(sourceName: String): String = {
    if (sourceName.contains('.')) {
      sourceName
    } else {
      s"default.$sourceName"
    }
  }

  /**
    * getYarnClusterName - gets the yarn cluster from the hadoop config file
    *
    * @return
    */
  def getYarnClusterName(): String = {
    val hadoopConfiguration = new org.apache.hadoop.conf.Configuration()
    val cluster = hadoopConfiguration.get(GimelConstants.FS_DEFAULT_NAME)
    cluster.split("/").last
  }

  /**
    * Determine Supported Properties
    * Currently Supported :
    * Map[String, Any]
    * String Example """hdfs.path=/sys/datalake;hdfs.file.format=parquet;hdfs.target.compress=true"""
    *
    * @param x Properties
    * @return Map[String, Any]
    */
  def getProps(x: Any): Map[String, Any] = {
    x match {
      case map: Map[String, Any] =>
        map
      case str: String =>
        propStringToMap(str)
      case _ =>
        val examplesString =
          """|
            |hdfs.path=/sys/datalake;hdfs.file.format=parquet;hdfs.target.compress=true
            | """.stripMargin.trim
        val errorMessageForClient =
          s"""
             |Invalid props type ${x.getClass.getCanonicalName}.
             |Supported types are eitherMap[String, String] OR String.
             |Valid example for String --> $examplesString
          """.stripMargin
        throw new Exception(errorMessageForClient)
    }
  }

  /**
    * Try to Convert a Key Value Pairs Text into a Map
    *
    * @param x KV pairs | Example """hdfs.path=/sys/datalake:hdfs.file.format=parquet:hdfs.target.compress=true"""
    * @return Map[String, Any]
    */
  def propStringToMap(x: String): Map[String, String] = {
    x.split(':').flatMap { keyValuePair =>
      keyValuePair.split('=') match {
        case Array("") =>
          None
        case Array(key, value) =>
          Some(key -> value)
        case _ =>
          val examplesString =
            """
              |hdfs.path=/sys/datalake;hdfs.file.format=parquet;hdfs.target.compress=true
              | """.stripMargin.trim
          val errorMessageForClient =
            s"""
               |Error While Parsing Key=Value Pairs in $x. Unable to convert the given string to pair!
               |Valid Example --> Example --> $examplesString
          """.stripMargin
          throw new Exception(errorMessageForClient)
      }
    }.toMap
  }

  /**
    * Gives an Unique App Tag for given execution
    *
    * @param sparkContext SparkContext
    * @return Application Tag String
    */
  def getAppTag(sparkContext: SparkContext): String = {
    val user = getUserName(sparkContext)
    val sparkAppName = getSparkAppName(sparkContext)
    val clusterName = getYarnClusterName()
    val appTag = s"${clusterName}/${user}/${sparkAppName}".replaceAllLiterally(" ", "-")
    appTag
  }

  /**
    * Gives a Unique Structured Streaming checkpoint Location on hdfs
    *
    * @param sparkContext SparkContext
    * @return Checkpoint Location
    */
  def getStructuredStreamingCheckpointLocation(sparkContext: SparkContext): String = {
    val user = getUserName(sparkContext)
    val sparkAppName = getSparkAppName(sparkContext)
    val checkpointLocation = s"/user/${user}/gimel_spark_app_kafka_checkpoint/${sparkAppName}".replaceAllLiterally(" ", "-")
    checkpointLocation
  }

  /**
    * Gives User Name
    *
    * @param sparkContext SparkContext
    * @return User Name
    */
  def getUserName(sparkContext: SparkContext): String = {
    val user = sparkContext.getLocalProperty(GimelConstants.GTS_USER_CONFIG) match {
      case null => {
        sys.env("USER")
      }
      case _ => {
        sparkContext.getLocalProperty(GimelConstants.GTS_USER_CONFIG)
      }
    }
    user
  }

  /**
    * Gives Spark App Name
    *
    * @param sparkContext SparkContext
    * @return Spark App Name
    */
  def getSparkAppName(sparkContext: SparkContext): String = {
    sparkContext.getConf.get(GimelConstants.SPARK_APP_NAME)
  }

  /**
    * Gives date in yyyy-MM-dd HH:mm:ss for a given timestamp
    *
    * @param timestamp String
    * @return String
    */
  def getDateFromTimestamp(timestamp: String): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("US/Pacific"))
    sdf.format(new Date(timestamp.toLong))
  }

  /**
    * Given the dataset name, it queries UDC for getting the appropriate connection information and returns back
    *
    * @param datasetName -> udc.teradata.mycluster.flightsdb.flights
    * @return
    */
  def getJdbcConnectionOptionsFromDataset(datasetName: String, dataSetProps: Map[String, Any]): Map[String, String] = {
    logger.info(s"@Begin --> ${new Exception().getStackTrace.apply(1).getMethodName}")
    logger.info(s"incoming dataset name: $datasetName")
    Try {
      getJdbcConnectionOptions(QueryParserUtils.extractSystemFromDatasetName(datasetName), dataSetProps)
    }.getOrElse(Map.empty)
  }

  /**
    * Given the system name, it queries UDC for getting the appropriate connection information and returns back
    *
    * @param storageSystemName -> Name of the JDBC system
    * @return
    */
  def getJdbcConnectionOptions(storageSystemName: String,
                               dataSetProps: Map[String, Any] = Map.empty): Map[String, String] = {
    logger.info(s"@Begin --> ${new Exception().getStackTrace.apply(1).getMethodName}")
    logger.info(s"incoming SystemName: $storageSystemName")
    // Throw validation error if cannot find the required information from UDC
    val storageSystemProperties = CatalogProvider.getStorageSystemProperties(storageSystemName)
    require(storageSystemProperties != null && storageSystemProperties.nonEmpty,
      s"Expecting the storage system properties for system: $storageSystemName to be available")
    require(storageSystemProperties.get(GimelConstants.STORAGE_TYPE).isDefined && storageSystemProperties(
      GimelConstants.STORAGE_TYPE).equalsIgnoreCase("JDBC"),
      s"Expecting ${GimelConstants.STORAGE_TYPE} to be available in dataset properties: $storageSystemProperties")
    Try {
      val filteredJdbcOptions = dataSetProps.filter(
        key => (key._1.contains(GimelConstants.GIMEL_JDBC_OPTION_KEYWORD) || key
          ._1.equalsIgnoreCase(GimelConstants.JDBC_CHARSET_KEY))
      ).map(kv => kv._1 -> kv._2.toString)
      val combinedJdbcOptions = filteredJdbcOptions ++ storageSystemProperties
      logger.info(s"Combined JDBC options for Storage system :$storageSystemName -> $combinedJdbcOptions")
      combinedJdbcOptions
    }.getOrElse(Map.empty)
  }

  /**
    * Fetch the Type of DataSetType based on the DataSetProperties that is Supplied
    *
    * @param dataSetProperties DataSetProperties
    * @return DataSetType
    */
  def getSystemType(dataSetProperties: DataSetProperties): (DataSetType.Value) = {
    dataSetProperties.datasetType.toUpperCase() match {
      case "HBASE" =>
        DataSetType.HBASE
      case "KAFKA" =>
        val kafkaApiVersion = GenericUtils.getValue(dataSetProperties.props,
          GimelConstants.GIMEL_KAFKA_VERSION, defaultValue = GimelConstants.GIMEL_KAFKA_DEFAULT_VERSION)
        if (kafkaApiVersion.equals(GimelConstants.GIMEL_KAFKA_VERSION_ONE)) {
          DataSetType.KAFKA
        } else {
          DataSetType.KAFKA2
        }
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
      case "RESTAPI" =>
        DataSetType.RESTAPI
      case "HIVE" =>
        DataSetType.HIVE
      case "SFTP" =>
        DataSetType.SFTP
      case _ =>
        DataSetType.HIVE
    }
  }

  /**
    * Checks whether the dataSet is HIVE by scanning the pcatalog phrase and also expecting to have the db and table
    * names to decide it is a HIVE table
    *
    * @param dataSet DataSet
    * @return Boolean
    */
  def isStorageTypeUnknown
  (dataSet: String): Boolean = {
    dataSet.split('.').head.toLowerCase() != GimelConstants.PCATALOG_STRING && dataSet.split('.').length == 2
  }

  /**
    * For a given dataset (table) this function calls getDataSetProperties which calls catalog provider and returns the dataset properties
    * which will be used to identify the storage of the dataset
    *
    * @param datasetName  - Incoming dataset
    * @param sparkSession - spark session
    * @param options      - Set of user options
    * @return - Returns the storage system (hive/teradata/kafka...)
    */
  def getSystemType(datasetName: String, sparkSession: SparkSession,
                    options: Map[String, String]): com.paypal.gimel.common.utilities.DataSetType.Value = {
    logger.info("Data set name is  ==> " + datasetName)
    val formattedProps: Map[String, Any] = getProps(options) ++
      Map(CatalogProviderConfigs.CATALOG_PROVIDER ->
        sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER,
          CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER))

    // if storage type unknown we will default to HIVE PROVIDER
    if (isStorageTypeUnknown(datasetName)) {
      formattedProps ++ Map(CatalogProviderConfigs.CATALOG_PROVIDER -> CatalogProviderConstants.HIVE_PROVIDER)
    }

    val dataSetProperties: DataSetProperties = CatalogProvider.getDataSetProperties(datasetName, options)
    logger.info("dataSetProperties  ==> " + dataSetProperties.toString())
    val systemType = getSystemType(dataSetProperties)
    systemType
  }

  /**
   * Sets log level based on the input from user
   *
   * @param sparkSession
   * @param logger
   */
  def setGimelLogLevel(sparkSession: SparkSession, logger: Logger) : Unit = {
    var gimelLoggingLevel = GenericUtils.getValue(sparkSession.conf.getAll,
      GimelConstants.LOG_LEVEL, GimelConstants.DEFAULT_LOG_LEVEL)
    logger.setLogAudit(GenericUtils.getValue(sparkSession.conf.getAll,
      GimelConstants.LOG_AUDIT_ENABLED, "false").toBoolean)
    gimelLoggingLevel = gimelLoggingLevel match {
      case "INFO" | "CONSOLE" =>
        logger.consolePrintEnabled = true
        "INFO"
      case "DEBUG" =>
        logger.consolePrintEnabled = true
        gimelLoggingLevel
      case _ =>
        logger.consolePrintEnabled = false
        gimelLoggingLevel
    }

    logger.setLogLevel(gimelLoggingLevel)
  }
}
