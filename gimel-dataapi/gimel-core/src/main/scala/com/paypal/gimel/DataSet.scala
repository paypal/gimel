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
import scala.reflect.runtime.universe._
import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import com.paypal.gimel.common.catalog.CatalogProvider
import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf._
import com.paypal.gimel.common.gimelserde.GimelSerdeUtils
import com.paypal.gimel.common.utilities._
import com.paypal.gimel.common.utilities.BindToFieldsUtils._
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.kafka.conf.KafkaConstants
import com.paypal.gimel.logger.Logger

class DataSet(val sparkSession: SparkSession) {

  import com.paypal.gimel.common.utilities.DataSetUtils._

  val user = sys.env("USER")
  val sparkAppName = sparkSession.conf.get(GimelConstants.SPARK_APP_NAME)
  val clusterName = getYarnClusterName()
  val appTag = getAppTag(sparkSession.sparkContext)
  val logger = Logger(this.getClass.getName)
  logger.setSparkVersion(sparkSession.version)
  val sparkContext: SparkContext = sparkSession.sparkContext
  val sqlContext: SQLContext = sparkSession.sqlContext
  var latestDataSetReader: Option[GimelDataSet] = None
  var latestDataSetWriter: Option[GimelDataSet] = None
  val currentTime = System.currentTimeMillis().toString
  val loadTag = appTag + "_" + currentTime
  sparkSession.sparkContext.setLogLevel("ERROR")
  var datasetSystemType = ""
  var additionalPropsToLog: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()

  // get gimel timer object
  val gimelTimer = Timer()

  import DataSetUtils._

  def latestKafkaDataSetReader: Option[GimelDataSet] = {
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
          GimelConstants.SPARK_APP_NAME -> sparkSession.conf.get(GimelConstants.SPARK_APP_NAME),
          GimelConstants.SPARK_APP_ID -> sparkSession.conf.get(GimelConstants.SPARK_APP_ID),
          GimelConstants.APP_TAG -> appTag)

      // if storage type unknown we will default to HIVE PROVIDER
      if (isStorageTypeUnknown(dataSet)) {
        formattedProps ++ Map(CatalogProviderConfigs.CATALOG_PROVIDER -> CatalogProviderConstants.HIVE_PROVIDER)
      }

      val dataSetProperties: DataSetProperties =
        CatalogProvider.getDataSetProperties(dataSet, formattedProps)
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
      // additionalPropsToLog = propsToLog
      datasetSystemType = systemType.toString

      val fieldsBindToStringProp = newProps.getOrElse(GimelConstants.FIELDS_BIND_TO_JSON,
        dataSetProperties.props.getOrElse(GimelConstants.FIELDS_BIND_TO_JSON, "")).toString
      val fieldsBindToDataset = newProps.getOrElse(GimelConstants.FIELDS_BIND_TO_DATASET,
        dataSetProperties.props.getOrElse(GimelConstants.FIELDS_BIND_TO_DATASET, "")).toString

      val (fieldsBindTo, fieldsBindToString) = getFieldsBindTo(fieldsBindToStringProp, fieldsBindToDataset, formattedProps)

      // Passing FIELDS_BIND_TO_JSON property for any other connector like kafka to reuse
      val data = this.read(systemType, dataSet, newProps ++ Map(GimelConstants.FIELDS_BIND_TO_JSON -> fieldsBindToString))

      // To deserialize the resultant Dataframe if serializer class is given (Using Class Loader)
      val deserializerClassName = newProps.getOrElse(GimelConstants.GIMEL_DESERIALIZER_CLASS,
        dataSetProperties.props.getOrElse(GimelConstants.GIMEL_DESERIALIZER_CLASS, "")).toString

      val deserializedData = if (deserializerClassName.isEmpty) {
        data
      } else {
        val deserializerObj = GimelSerdeUtils.getDeserializerObject(deserializerClassName)
        deserializerObj.deserialize(data, dataSetProperties.props ++ newProps)
      }

      val finalData = getDFBindToFields(deserializedData, fieldsBindTo)

      // update log variables to push logs
      val endTime = gimelTimer.endTime.get
      val executionTime: Double = gimelTimer.endWithMillSecRunTime

      // post audit logs to KAFKA
      logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkAppName
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeBatch
        , clusterName
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
      finalData
    }
    catch {
      case e: Throwable =>
        logger.error(s"Pushing to logs:  Error Description\n dataset=${dataSet}\n method=${MethodName}\n Error: ${e.printStackTrace()}")

        // update log variables to push logs
        val endTime = System.currentTimeMillis()
        val executionTime = endTime - startTime

        // post audit logs to KAFKA
        logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkAppName
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeBatch
          , clusterName
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

        val msg = s"Error in DataSet ${MethodName} Operation. Common Gimel 'Exceptions' are explained here : http://go/gimel/exceptions"
        throw new DataSetOperationException(e.toString + "\n" + msg, e)
    }
  }

  /**
    * Client API : Calls appropriate DataSet & its Write method
    *
    * @param dataSet   Example : flights | flights | default.flights
    * @param dataFrame DataFrame
    * @param props     Additional Properties for the Reader of Dataset
    * @return DataFrame
    */
  def write(dataSet: String, dataFrame: DataFrame
            , props: Any = Map[String, Any]()): DataFrame = {
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
          GimelConstants.SPARK_APP_NAME -> sparkSession.conf.get(GimelConstants.SPARK_APP_NAME),
          GimelConstants.SPARK_APP_ID -> sparkSession.conf.get(GimelConstants.SPARK_APP_ID),
          GimelConstants.APP_TAG -> appTag)

      // if storage type unknown we will default to HIVE PROVIDER
      if (isStorageTypeUnknown(dataSet)) {
        formattedProps ++ Map(CatalogProviderConfigs.CATALOG_PROVIDER -> CatalogProviderConstants.HIVE_PROVIDER)
      }
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
      // additionalPropsToLog = propsToLog
      datasetSystemType = systemType.toString

      // To deserialize the resultant Dataframe if serializer class is given (Using Class Loader)
      val serializerClassName = newProps.getOrElse(GimelConstants.GIMEL_SERIALIZER_CLASS,
        dataSetProperties.props.getOrElse(GimelConstants.GIMEL_SERIALIZER_CLASS, "")).toString

      val serializedData = if (serializerClassName.isEmpty) {
        dataFrame
      } else {
        val serializerObj = GimelSerdeUtils.getSerializerObject(serializerClassName)
        serializerObj.serialize(dataFrame, dataSetProperties.props ++ newProps)
      }

      this.write(systemType, dataSet, serializedData, newProps)

      // update log variables to push logs
      val endTime = System.currentTimeMillis()
      val executionTime = endTime - startTime

      // post audit logs to KAFKA
      logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkAppName
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeBatch
        , clusterName
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

      dataFrame

    }
    catch {
      case e: Throwable =>
        logger.error(s"Error Description\n dataset=${dataSet}\n method=${MethodName}\n Error: ${e.printStackTrace()}")
        // update log variables to push logs
        val endTime = gimelTimer.endTime.get
        val executionTime = gimelTimer.endWithMillSecRunTime

        // post audit logs to KAFKA
        logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkAppName
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeBatch
          , clusterName
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

        val msg = s"Error in DataSet ${MethodName} Operation. Common Gimel 'Exceptions' are explained here : http://go/gimel/exceptions"
        throw new DataSetOperationException(e.toString + "\n" + msg, e)
    }

  }

  /**
    * Client API : Calls appropriate DataSet & its Write method
    *
    * @param dataSet Example : flights | flights_time | default.flights
    * @param anyRDD  RDD[T] UnSupported Types may fail at run-time.
    *                Please check Documentation of APIs carefully.
    * @param props   Additional Properties for the Reader of Dataset
    * @return
    */

  def write[T: TypeTag](dataSet: String, anyRDD: RDD[T], props: Any): RDD[T] = {
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
      // additionalPropsToLog = propsToLog
      datasetSystemType = systemType.toString

      // update log variables to push logs
      val endTime = gimelTimer.endTime.get
      val executionTime = gimelTimer.endWithMillSecRunTime

      // post audit logs to KAFKA
      logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkAppName
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeBatch
        , clusterName
        , user
        , appTag.replaceAllLiterally("/", "_")
        , MethodName
        , dataSet
        , datasetSystemType
        , ""
        , additionalPropsToLog
        , GimelConstants.SUCCESS
        , GimelConstants.UNKNOWN_STRING
        , GimelConstants.UNKNOWN_STRING
        , startTime
        , endTime
        , executionTime
      )
      anyRDD
    }
    catch {
      case e: Throwable =>
        logger.error(s"Error Description\n dataset=${dataSet}\n method=${MethodName}\n Error: ${e.printStackTrace()}")
        // update log variables to push logs
        val endTime = System.currentTimeMillis()
        val executionTime = endTime - startTime

        // post audit logs to KAFKA
        logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkAppName
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeBatch
          , clusterName
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

        val msg = s"Error in DataSet ${MethodName} Operation. Common Gimel 'Exceptions' are explained here : http://go/gimel/exceptions"
        throw new DataSetOperationException(msg, e)
    }

  }

  /**
    * Calls appropriate DataSet & its read method
    *
    * @param sourceType Example : KAFKA | ELASTIC_SEARCH | HDFS | HBASE  |JDBC
    * @param sourceName Example : flights | flights_time | default.flights
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
    * @param targetName Example : flights | flights_time | default.flights
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
    * @param targetName Example : flights | flights_time | default.flights
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

  /**
    * Calls appropriate DataSet & its create method
    *
    * @param sourceType Example : KAFKA | ELASTIC_SEARCH | HDFS | HBASE  |JDBC
    * @param sourceName Example : flights | flights_time | default.flights
    * @param props      Additional Properties for the Reader of Dataset
    * @return DataFrame
    */
  private def create(sourceType: DataSetType.SystemType
                     , sourceName: String
                     , props: Map[String, Any]): Unit = {
    latestDataSetReader = Some(getDataSet(sparkSession, sourceType))
    latestDataSetReader.get.create(sourceName, props)
  }

  /**
    * Identifies the catalog provider and gets the system properties and prepares DataSetProperties and
    * call the wrapper create to decide the respective storage create to be called
    *
    * @param dataSet DataSet Name | DB.TABLE | Example : default.temp
    * @param props   Additional Properties for the Reader of Dataset
    * @return Boolean
    */
  def create(dataSet: String, props: Map[String, Any]): Unit = {
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

      sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER) match {
        case com.paypal.gimel.common.conf.CatalogProviderConstants.HIVE_PROVIDER =>
          throw new Exception(s"HIVE Provider is NOT currently Supported")
        case _ => None
      }

      val datasetProps: DataSetProperties =
        CatalogProvider.getDataSetProperties(dataSet, formattedProps)
      val systemType = getSystemType(datasetProps)
      val newProps: Map[String, Any] = getProps(props) ++ Map(
        GimelConstants.DATASET_PROPS -> datasetProps
        , GimelConstants.DATASET -> dataSet
        , GimelConstants.RESOLVED_HIVE_TABLE -> resolveDataSetName(dataSet)
        , GimelConstants.APP_TAG -> appTag)
      // Why are we doing this? Elastic Search Cannot Accept "." in keys
      val dataSetProps = datasetProps.props.map { case (k, v) =>
        k.replaceAllLiterally(".", "~") -> v
      }
      val propsToLog = scala.collection.mutable.Map[String, String]()
      dataSetProps.foreach(x => propsToLog.put(x._1, x._2))
      // additionalPropsToLog = propsToLog
      datasetSystemType = systemType.toString

      this.create(systemType, dataSet, newProps)

      // update log variables to push logs
      val endTime = System.currentTimeMillis()
      val executionTime = endTime - startTime

      // post audit logs to KAFKA
      logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkAppName
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeBatch
        , clusterName
        , user
        , appTag.replaceAllLiterally("/", "_")
        , MethodName
        , dataSet
        , datasetSystemType
        , ""
        , additionalPropsToLog
        , GimelConstants.SUCCESS
        , GimelConstants.UNKNOWN_STRING
        , GimelConstants.UNKNOWN_STRING
        , startTime
        , endTime
        , executionTime
      )
    }
    catch {
      case e: Throwable =>

        logger.error(s"Error Description\n dataset=${dataSet}\n method=${MethodName}\n Error: ${e.printStackTrace()}")

        // update log variables to push logs
        val endTime = gimelTimer.endTime.get
        val executionTime = gimelTimer.endWithMillSecRunTime

        // post audit logs to KAFKA
        logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkAppName
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeBatch
          , clusterName
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

        val msg = s"Error in DataSet ${MethodName} Operation. Common Gimel 'Exceptions' are explained here : http://go/gimel/exceptions"
        throw new DataSetOperationException(e.getMessage + "\n" + msg, e)
    }
  }

  /**
    * Calls appropriate DataSet & its drop method
    *
    * @param sourceType Example : KAFKA | ELASTIC_SEARCH | HDFS | HBASE  |JDBC
    * @param sourceName Example : flights | flights_time | default.flights
    * @param props      Additional Properties for the Reader of Dataset
    * @return DataFrame
    */
  private def drop(sourceType: DataSetType.SystemType
                   , sourceName: String
                   , props: Map[String, Any]): Unit = {
    latestDataSetReader = Some(getDataSet(sparkSession, sourceType))
    latestDataSetReader.get.drop(sourceName, props)
  }

  /**
    * Identifies the catalog provider and gets the system properties and prepares DataSetProperties and
    * call the wrapper drop to decide the respective storage drop to be called
    *
    * @param dataSet DataSet Name | DB.TABLE | Example : default.temp
    * @param props   Additional Properties for the Reader of Dataset
    * @return Boolean
    */
  def drop(dataSet: String, props: Map[String, Any]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    // get start time
    val startTime = gimelTimer.start.get

    try {

      // Set gimel log level and flag to audit logs to kafka
      setGimelLogLevel(sparkSession, logger)

      val formattedProps: Map[String, Any] = getProps(props) ++
        Map(CatalogProviderConfigs.CATALOG_PROVIDER ->
          sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER,
            CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER),
          GimelConstants.SPARK_APP_ID -> sparkSession.conf.get(GimelConstants.SPARK_APP_ID),
          GimelConstants.SPARK_APP_NAME -> sparkSession.conf.get(GimelConstants.SPARK_APP_NAME),
          GimelConstants.APP_TAG -> appTag)

      sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER) match {
        case com.paypal.gimel.common.conf.CatalogProviderConstants.HIVE_PROVIDER =>
          throw new Exception(s"HIVE Provider is NOT currently Supported")
        case _ => None
      }
      // val resolvedSourceTable = resolveDataSetName(dataSet)
      val datasetProps: DataSetProperties =
        CatalogProvider.getDataSetProperties(dataSet, formattedProps)
      val systemType = getSystemType(datasetProps)
      val newProps: Map[String, Any] = getProps(props) ++ Map(
        GimelConstants.DATASET_PROPS -> datasetProps
        , GimelConstants.DATASET -> dataSet
        , GimelConstants.RESOLVED_HIVE_TABLE -> resolveDataSetName(dataSet)
        , GimelConstants.APP_TAG -> appTag)
      // Why are we doing this? Elastic Search Cannot Accept "." in keys
      val dataSetProps = datasetProps.props.map { case (k, v) =>
        k.replaceAllLiterally(".", "~") -> v
      }
      val propsToLog = scala.collection.mutable.Map[String, String]()
      dataSetProps.foreach(x => propsToLog.put(x._1, x._2))
      // additionalPropsToLog = propsToLog
      datasetSystemType = systemType.toString

      this.drop(systemType, dataSet, newProps)

      // update log variables to push logs
      val endTime = System.currentTimeMillis()
      val executionTime = endTime - startTime

      // post audit logs to KAFKA
      logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkAppName
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeBatch
        , clusterName
        , user
        , appTag.replaceAllLiterally("/", "_")
        , MethodName
        , dataSet
        , datasetSystemType
        , ""
        , additionalPropsToLog
        , GimelConstants.SUCCESS
        , GimelConstants.UNKNOWN_STRING
        , GimelConstants.UNKNOWN_STRING
        , startTime
        , endTime
        , executionTime
      )
    }
    catch {

      case e: Throwable =>

        logger.error(s"Error Description\n dataset=${dataSet}\n method=${MethodName}\n Error: ${e.printStackTrace()}")

        // update log variables to push logs
        val endTime = gimelTimer.endTime.get
        val executionTime = gimelTimer.endWithMillSecRunTime

        // post audit logs to KAFKA
        logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkAppName
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeBatch
          , clusterName
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

        logger.throwError(e.toString)

        val msg = s"Error in DataSet ${MethodName} Operation. Common Gimel 'Exceptions' are explained here : http://go/gimel/exceptions"
        throw new DataSetOperationException(e.getMessage + "\n" + msg, e)
    }
  }

  /**
    * Calls appropriate DataSet & its truncate method
    *
    * @param sourceType Example : KAFKA | ELASTIC_SEARCH | HDFS | HBASE  |JDBC
    * @param sourceName Example : flights | flights_time | default.flights
    * @param props      Additional Properties for the Reader of Dataset
    * @return DataFrame
    */
  private def truncate(sourceType: DataSetType.SystemType
                       , sourceName: String
                       , props: Map[String, Any]): Unit = {
    latestDataSetReader = Some(getDataSet(sparkSession, sourceType))
    latestDataSetReader.get.truncate(sourceName, props)
  }

  /**
    * Identifies the catalog provider and gets the system properties and prepares DataSetProperties and
    * call the wrapper truncate to decide the respective storage truncate to be called
    *
    * @param dataSet DataSet Name | DB.TABLE | Example : default.temp
    * @param props   Additional Properties for the Reader of Dataset
    * @return Boolean
    */
  def truncate(dataSet: String, props: Map[String, Any]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    // get start time
    val startTime = gimelTimer.start.get

    try {

      // Set gimel log level and flag to audit logs to kafka
      setGimelLogLevel(sparkSession, logger)

      val formattedProps: Map[String, Any] = getProps(props) ++
        Map(CatalogProviderConfigs.CATALOG_PROVIDER ->
          sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER,
            CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER),
          GimelConstants.SPARK_APP_ID -> sparkSession.conf.get(GimelConstants.SPARK_APP_ID),
          GimelConstants.SPARK_APP_NAME -> sparkSession.conf.get(GimelConstants.SPARK_APP_NAME),
          GimelConstants.APP_TAG -> appTag)
      sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER) match {
        case com.paypal.gimel.common.conf.CatalogProviderConstants.HIVE_PROVIDER =>
          throw new Exception(s"HIVE Provider is NOT currently Supported")
        case _ => None
      }
      val datasetProps: DataSetProperties =
        CatalogProvider.getDataSetProperties(dataSet, formattedProps)
      val systemType = getSystemType(datasetProps)
      val newProps: Map[String, Any] = getProps(props) ++ Map(
        GimelConstants.DATASET_PROPS -> datasetProps
        , GimelConstants.DATASET -> dataSet
        , GimelConstants.RESOLVED_HIVE_TABLE -> resolveDataSetName(dataSet)
        , GimelConstants.APP_TAG -> appTag)
      // Why are we doing this? Elastic Search Cannot Accept "." in keys
      val dataSetProps = datasetProps.props.map { case (k, v) =>
        k.replaceAllLiterally(".", "~") -> v
      }
      val propsToLog = scala.collection.mutable.Map[String, String]()
      dataSetProps.foreach(x => propsToLog.put(x._1, x._2))
      // additionalPropsToLog = propsToLog
      datasetSystemType = systemType.toString

      this.truncate(systemType, dataSet, newProps)

      // update log variables to push logs
      val endTime = System.currentTimeMillis()
      val executionTime = endTime - startTime

      // post audit logs to KAFKA
      logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkAppName
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeBatch
        , clusterName
        , user
        , appTag.replaceAllLiterally("/", "_")
        , MethodName
        , dataSet
        , datasetSystemType
        , ""
        , additionalPropsToLog
        , GimelConstants.SUCCESS
        , GimelConstants.UNKNOWN_STRING
        , GimelConstants.UNKNOWN_STRING
        , startTime
        , endTime
        , executionTime
      )
    }
    catch {

      case e: Throwable =>

        logger.error(s"Error Description\n dataset=${dataSet}\n method=${MethodName}\n Error: ${e.printStackTrace()}")

        // update log variables to push logs
        val endTime = gimelTimer.endTime.get
        val executionTime = gimelTimer.endWithMillSecRunTime

        // post audit logs to KAFKA
        logger.logApiAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkAppName
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeBatch
          , clusterName
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

        val msg = s"Error in DataSet ${MethodName} Operation. Common Gimel 'Exceptions' are explained here : http://go/gimel/exceptions"
        throw new DataSetOperationException(e.getMessage + "\n" + msg, e)
    }

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
    * provides an appropriate gimel DataSet
    *
    * @param sparkSession : SparkSession
    * @param sourceType   Type of System. Example - HBASE , ES, KAFKA, HDFS, MYSQL
    * @return Gimel DataSet
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
      case DataSetType.RESTAPI =>
        new com.paypal.gimel.restapi.DataSet(sparkSession)
      case DataSetType.DRUID =>
        new com.paypal.gimel.druid.DataSet(sparkSession)
      case DataSetType.SFTP =>
        new com.paypal.gimel.sftp.DataSet(sparkSession)
      case DataSetType.KAFKA2 =>
        new com.paypal.gimel.kafka2.DataSet(sparkSession)
    }
  }

  /**
    * Gets the last user Kafka DataSet reader (if already use), else Returns None
    *
    * @param dataSet DataSet
    * @return Option[KafkaDataSet]
    */

  def getLatestKafkaDataSetReader(dataSet: DataSet): Option[GimelDataSet] = {
    Try {
      dataSet.latestDataSetReader.get match {
        case kafka: com.paypal.gimel.kafka.DataSet =>
          kafka
        case kafka2: com.paypal.gimel.kafka2.DataSet =>
          kafka2
      }
    }.toOption
  }
}

/**
  * Custom Exception for Dataset Operation initiation errors.
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class DataSetOperationException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}
