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

package com.paypal.gimel.testsuite

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import spray.json._
import spray.json.DefaultJsonProtocol._

import com.paypal.gimel.DataSet
import com.paypal.gimel.common.conf.{GimelConstants, GimelProperties}
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs
import com.paypal.gimel.kafka.utilities.KafkaUtilities
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.testsuite.conf.TestSuiteConstants
import com.paypal.gimel.testsuite.storages._
import com.paypal.gimel.testsuite.utilities.{GimelTestSuiteProperties, HiveJDBCUtilsForTestSuite}

object TestSuite extends App {

  // Logger Initiation
  val logger = Logger(this.getClass.getName)
  logger.consolePrintEnabled = true
  val sparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  val sc = sparkSession.sparkContext
  sc.setLogLevel("ERROR")
  val sqlContext = sparkSession.sqlContext

  /**
    * --- All the implementation is in SmokeTestUtils. Initiating Utils. ----
    */
  val utils = new TestUtils(args, sparkSession)

  /**
    * --- Creates Hive DataBase if it does not exists already ----
    */
  utils.bootStrapHiveDB()
  logger.info("DataBase BootStrapped.")

  /**
    * ---- This is the section where every storage will be validated
    */
  utils.storagesToValidate.foreach { storage =>
    logger.info(s"Invoking Storage Validation From Factory for --> $storage")
    val (ddlResult, statsResult) = utils.getFromValidationFactory(storage).execute()
    ddlResult.foreach(x => utils.ddls += x)
    statsResult.foreach(x => utils.stats += x)
  }

  /**
    * --- Summarize Stats ----
    */
  val allStats: Map[String, String] = utils.stats ++ utils.ddls
  val statsJSON = allStats.toJson
  utils.ddls.foreach(x => logger.info(s"BootStrap Object Definitions --> \n ${x._1} --> ${x._2}"))
  Logger(this.getClass.getName).info("SmokeTest Summary -->")
  Logger(this.getClass.getName).info(statsJSON.compactPrint)

  /**
    * --- ES Index Creation & Post Summary ----
    */
  utils.bootStrapESIndexForStats()
  utils.postResults(allStats)

  /**
    * --- Drop Hive DB ----
    */
  //  utils.cleanUpHiveDB()

  Logger(this.getClass.getName).info("TEST SUITE - COMPLETED")
  // to do println statement should be removed
  // Once the kafka_stream validation completes and when we kill the streaming context,
  // the logger object is getting killed.Need to investogate moe on why this logger object is killed
  println("TEST SUITE - COMPLETED")
}

class TestUtils(val allParams: Array[String], val sparkSession: SparkSession) {

  // Get logger
  val logger = Logger()
  // DDLs will be filled in later based on runtime options
  var ddls: Map[String, String] = Map()
  // Stats will be filled in later based on runtime options
  var stats: Map[String, String] = Map()
  // User Provided parameters resolved here, with defaults on the mandatory parameters
  val params: Map[String, String] = resolveRunTimeParameters(allParams)
  // Parse to determine the storage that need to be validated/tested
  val storagesToValidate: Array[String] = params("storages").split(",")
  // Default Pcatalog Properties are resolved with user supplied properties
  val gimelProps = GimelTestSuiteProperties(params)
  // Initiate Kafka Utilities to get some Kafka Client / Admin Operations
  val kafkaUtils = KafkaUtilities
  // HiveJDBC Utils for Hive deployment
  val hiveJDBCUtils = HiveJDBCUtilsForTestSuite(gimelProps, gimelProps.cluster)

  // Initiate DataSet
  val dataset = DataSet(sparkSession)
  // Hive Jars needed for DDL Deployment
  val hiveJarsForDDL: String = gimelProps.gimelHiveJarsToAdd

  /**
    * Resolves RunTime Params
    *
    * @param allParams args
    * @return Map[String, String]
    */
  def resolveRunTimeParameters(allParams: Array[String]): Map[String, String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    var paramsMapBuilder: Map[String, String] = Map()
    logger.info(s"All Params From User --> ${allParams.mkString("\n")}")

    for (jobParams <- allParams) {
      for (eachParam <- jobParams.split(" ")) {
        paramsMapBuilder += (eachParam.split("=")(0) -> eachParam.split("=", 2)(1))
      }
    }
    if (!paramsMapBuilder.contains(TestSuiteConstants.storagesToBenchmarkKey)) paramsMapBuilder += (TestSuiteConstants.storagesToBenchmarkKey -> "kafka_cdh,kafka_avro,kafka_string,elasticsearch,hbase,hive,teradata_jdbc,teradata_bulkload,kafka_stream,aerospike")
    if (!paramsMapBuilder.contains(TestSuiteConstants.topicKey)) paramsMapBuilder += (TestSuiteConstants.topicKey -> "flights.flights_log")
    if (!paramsMapBuilder.contains(TestSuiteConstants.fetchRowsKey)) paramsMapBuilder += (TestSuiteConstants.fetchRowsKey -> "100")
    if (!paramsMapBuilder.contains(TestSuiteConstants.maxRecordsPerPartitionKey)) paramsMapBuilder += (TestSuiteConstants.maxRecordsPerPartitionKey -> "1000000")
    if (!paramsMapBuilder.contains(TestSuiteConstants.minRowsPerPartitionKey)) paramsMapBuilder += (TestSuiteConstants.minRowsPerPartitionKey -> "100000")
    logger.info(s"Resolved Params From Code --> $paramsMapBuilder")
    stats += ("Params" -> paramsMapBuilder.mkString("\n"))
    paramsMapBuilder
  }

  /**
    * Returns a StorageValidation From Factory of Storages for Validation
    *
    * @param storageType Storage String such as "kafka", "cdh", "hive" , "elasticsearch", "hbase" & more to come in future
    * @return StorageValidation
    */
  def getFromValidationFactory(storageType: String): StorageValidation = {
    storageType.toLowerCase() match {
      case "kafka_avro" =>
        new KafkaAvroMessageValidation(dataset, sparkSession, gimelProps)
      case "kafka_json" =>
        new KafkaJSONMessageValidation(dataset, sparkSession, gimelProps)
      case "kafka_binary" =>
        new KafkaBinaryMessageValidation(dataset, sparkSession, gimelProps)
      case "kafka_string" =>
        new KafkaStringMessageValidation(dataset, sparkSession, gimelProps)
      case "elasticsearch" =>
        new ElasticSearchValidation(dataset, sparkSession, gimelProps)
      case "hive" =>
        new HiveValidation(dataset, sparkSession, gimelProps)
      case "hbase" =>
        new HBaseValidation(dataset, sparkSession, gimelProps)
      case "teradata_jdbc" =>
        new TeradataJDBCValidation(dataset, sparkSession, gimelProps)
      case "teradata_bulkload" =>
        new TeradataBulkLoadValidation(dataset, sparkSession, gimelProps)
      case "hbase_lookup" =>
        new HBaseLookUpValidation(dataset, sparkSession, gimelProps)
      case "kafka_stream" =>
        new KafkaStreamValidation(dataset, sparkSession, gimelProps)
      case "aerospike" =>
        new AerospikeValidation(dataset, sparkSession, gimelProps)
      case _ =>
        throw new Exception(s"UnSupport Storage Validation Expected From Client -> $storageType")
    }
  }

  /**
    * Creates the Hive DataBase for PCataLog
    */
  def bootStrapHiveDB(): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val hiveDDL =
        s""" create database if not exists ${gimelProps.smokeTestHiveDB}
            |location "${gimelProps.smokeTestHiveLocation}"
         """.stripMargin

      sparkSession.sql(hiveDDL)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("Hive DB" -> hiveDDL)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Creates An Index to Store the SmokeTestStats
    */

  def bootStrapESIndexForStats(): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val esDDL =
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS ${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestResultEsIndex}
           |(
           |  `data` string COMMENT 'from deserializer'
           |)
           |LOCATION
           |  '${gimelProps.smokeTestHiveLocation}/${gimelProps.smokeTestResultEsIndex}'
           |TBLPROPERTIES (
           |  '${GimelConstants.STORAGE_TYPE}'='ELASTIC_SEARCH',
           |  '${ElasticSearchConfigs.esIndexAutoCreate}'='true',
           |  '${GimelConstants.ES_NODE}'='${gimelProps.smokeTestResultEsHost}',
           |  '${GimelConstants.ES_PORT}'='${gimelProps.smokeTestResultEsPort}',
           |  '${ElasticSearchConfigs.esResource}'='${gimelProps.smokeTestResultEsIndex}/data'
           |)
      """.stripMargin

      // we are adding these jars because Hive Session needs these jar for executing the above DDL(It needs elasticsearch-hadoop jar for ESStorage Handler)
      // we are not using hiveContext.sql because spark 2.1 version doesnt support Stored by ES Storage Handler and Serde.so we are replacing with Hive JDBC as it supports both versions(1.6 and 2.1)

      deployDDL(esDDL)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("SmokeTestResultHiveTableDDL" -> esDDL)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Takes a Map of Stats, Constructs additional information on current run, Posts to ES
    *
    * @param stats Map[Key, Value]
    */
  def postResults(stats: Map[String, String]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val cluster = params.getOrElse("cluster", "unknown_cluster")
    val sessionName = sparkSession.sparkContext.appName
    val sessionUser = sparkSession.sparkContext.sparkUser
    val currTime = Calendar.getInstance().getTimeInMillis
    val statsKeys = Map(
      "runTime" -> currTime.toString
      , "cluster" -> cluster
      , "user" -> sessionUser
      , "runTag" -> sessionName
    )
    val toPostStats: Map[String, String] = statsKeys ++ stats
    val jsonStats: String = toPostStats.toJson.compactPrint
    val postableStats: RDD[String] = sparkSession.sparkContext.parallelize(Seq(jsonStats))
    val statsDF: DataFrame = sparkSession.read.json(postableStats)
    dataset.write(s"${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestResultEsIndex}", statsDF)
    logger.info(s"Stats --> $jsonStats")
    logger.info(s"Posted Stats to --> ${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestResultEsIndex}")
  }

  /**
    * Drops the Hive Database in the End
    */
  def cleanUpHiveDB(): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      sparkSession.sql(s"DROP DATABASE IF EXISTS ${gimelProps.smokeTestHiveDB} CASCADE")
      sparkSession.sql(s"dfs -rm -r -f ${gimelProps.smokeTestHiveLocation}")
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * One Place to Handle Exception
    *
    * @param ex      Throwable Exception
    * @param message A Custom Message
    */
  def handleException(ex: Throwable, message: String = ""): Nothing = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    //    cleanUpHiveDB()
    ex.printStackTrace()
    throw new Exception(s"An Error Occurred <$message> \n ${ex.getMessage}")
  }

  /**
    * Deploy DDLS
    *
    * @param executeDDL ddl string
    *
    */
  def deployDDL(executeDDL: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    hiveJDBCUtils.withStatement { statement =>
      hiveJarsForDDL.split(",").foreach { jarsToAdd =>
        statement.execute(s"ADD JAR $jarsToAdd")
      }
      statement.execute(executeDDL)
    }

  }

}





