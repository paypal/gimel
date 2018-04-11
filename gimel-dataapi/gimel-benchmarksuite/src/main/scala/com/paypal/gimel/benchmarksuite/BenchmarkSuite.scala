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

package com.paypal.gimel.benchmarksuite

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import spray.json._
import spray.json.DefaultJsonProtocol._

import com.paypal.gimel.DataSet
import com.paypal.gimel.benchmarksuite.conf.{BenchmarkSuiteConfigs, BenchmarkSuiteConstants}
import com.paypal.gimel.benchmarksuite.storages._
import com.paypal.gimel.benchmarksuite.utilities.GimelBenchmarkProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.kafka.utilities.KafkaUtilities
import com.paypal.gimel.logger.Logger

object BenchmarkSuite extends App with Logger {

  val sparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  val sc = sparkSession.sparkContext
  sc.setLogLevel("ERROR")
  val sqlContext = sparkSession.sqlContext

  /**
    * --- All the implementation is in BenchMarkUtils. Initiating Utils. ----
    */
  val utils = new BenchmarkTestUtils(args, sparkSession, sqlContext)

  /**
    * --- Creates Hive DataBase if it does not exists already ----
    */
  utils.bootStrapHiveDB()
  info("DataBase BootStrapped.")

  /**
    * ---- This is the section where every storage will be validated
    */
  utils.storagesToValidate.foreach { storage =>
    val testData = utils.prepareBenchMarkTestData(utils.noOfRows.toInt).cache()
    info(s"Invoking Storage Validation From Factory for --> $storage")
    val (ddlResult, statsResult) = utils.getFromValidationFactory(storage, testData).execute()
    ddlResult.foreach(x => utils.ddls += x)
    statsResult.foreach(x => utils.stats += x)
  }

  /**
    * --- Summarize Stats ----
    */
  val allStats = utils.stats ++ utils.ddls
  val statsJSON = allStats.toJson
  utils.ddls.foreach(x => info(s"BootStrap Object Definitions --> \n ${x._1} --> ${x._2}"))
  info("BenchmarkTest Summary -->")
  info(statsJSON.compactPrint)

  /**
    * --- Drop Hive DB ----
    */
  utils.cleanUpHiveDB()

  info("BENCHMARK  SUITE  - COMPLETED")
}

class BenchmarkTestUtils(val allParams: Array[String], val sparkSession: SparkSession, val sqlContext: SQLContext) extends Logger {

  // DDLs will be filled in later based on runtime options
  var ddls: Map[String, String] = Map()
  // Stats will be filled in later based on runtime options
  var stats: Map[String, String] = Map()
  // User Provided parameters resolved here, with defaults on the mandatory parameters
  val params: Map[String, String] = resolveRunTimeParameters(allParams)
  // Parse to determine the storage that need to be validated/tested
  val storagesToValidate: Array[String] = params("storages").split(",")
  // volume to benchmark
  val noOfRows: String = params(BenchmarkSuiteConfigs.sampleRowCount)
  // Default Pcatalog Properties are resolved with user supplied properties
  val pcatProps = GimelBenchmarkProperties(params)
  // Initiate Kafka Utilities to get some Kafka Client / Admin Operations
  val kafkaUtils = KafkaUtilities
  // Initiate DataSet
  val dataset = DataSet(sparkSession)

  var nativeAPIData: Map[String, String] = Map()
  var datasetAPIData: Map[String, String] = Map()

  // To get ClusterName
  private val hadoopConfiguration = new org.apache.hadoop.conf.Configuration()
  val clusterUrl: String = hadoopConfiguration.get(GimelConstants.FS_DEFAULT_NAME)
  val clusterName: String = new java.net.URI(clusterUrl).getHost


  /**
    * Resolves RunTime Params
    *
    * @param allParams args
    * @return Map[String, String]
    */
  def resolveRunTimeParameters(allParams: Array[String]): Map[String, String] = withMethdNameLogging { methodName =>
    var paramsMapBuilder: Map[String, String] = Map()
    info(s"All Params From User --> ${allParams.mkString("\n")}")

    for (jobParams <- allParams) {
      for (eachParam <- jobParams.split(" ")) {
        paramsMapBuilder += (eachParam.split("=")(0) -> eachParam.split("=", 2)(1))
      }
    }
    if (!paramsMapBuilder.contains(BenchmarkSuiteConstants.storagesToBenchmarkKey)) {
      paramsMapBuilder += (BenchmarkSuiteConstants.storagesToBenchmarkKey -> BenchmarkSuiteConstants.storagesToBenchmarkValue)
    }
    if (!paramsMapBuilder.contains(BenchmarkSuiteConstants.topicKey)) {
      paramsMapBuilder += (BenchmarkSuiteConstants.topicKey -> "flights.flights_log")
    }
    if (!paramsMapBuilder.contains(BenchmarkSuiteConstants.fetchRowsKey)) {
      paramsMapBuilder += (BenchmarkSuiteConstants.fetchRowsKey -> "100")
    }
    if (!paramsMapBuilder.contains(BenchmarkSuiteConstants.maxRecordsPerPartitionKey)) {
      paramsMapBuilder += (BenchmarkSuiteConstants.maxRecordsPerPartitionKey -> "1000000")
    }
    if (!paramsMapBuilder.contains(BenchmarkSuiteConstants.minRowsPerPartitionKey)) {
      paramsMapBuilder += (BenchmarkSuiteConstants.minRowsPerPartitionKey -> "100000")
    }
    if (!paramsMapBuilder.contains(BenchmarkSuiteConstants.clusterNameKey)) {
      paramsMapBuilder += (BenchmarkSuiteConstants.clusterNameKey -> clusterName)
    }
    if (!paramsMapBuilder.contains(BenchmarkSuiteConfigs.sampleRowCount)) {
      paramsMapBuilder += (BenchmarkSuiteConfigs.sampleRowCount -> "1000000")
    }

    info(s"Resolved Params From Code --> $paramsMapBuilder")
    stats += ("Params" -> paramsMapBuilder.mkString("\n"))
    paramsMapBuilder
  }

  /**
    * Returns a StorageValidation From Factory of Storages for Validation
    *
    * @param storageType Storage String such as "kafka", "cdh", "hive" , "elasticsearch", "hbase" & more to come in future
    * @return StorageValidation
    */
  def getFromValidationFactory(storageType: String, testData: DataFrame): StorageValidation = {
    storageType.toLowerCase() match {
      case "kafka_string" =>
        new KafkaStringMessageValidation(dataset, sparkSession, sqlContext, pcatProps, testData)
      case "kafka_avro" =>
        new KafkaAvroMessageValidation(dataset, sparkSession, sqlContext, pcatProps, testData)
      case "elasticsearch" =>
        new ElasticSearchValidation(dataset, sparkSession, sqlContext, pcatProps, testData)
      case "hive" =>
        new HiveValidation(dataset, sparkSession, sqlContext, pcatProps, testData)
      case "hbase" =>
        new HBaseValidation(dataset, sparkSession, sqlContext, pcatProps, testData)
      case _ =>
        throw new Exception(s"UnSupport Storage Validation Expected From Client -> $storageType")
    }
  }

  /**
    * Creates the Hive DataBase for PCataLog
    */
  def bootStrapHiveDB(): Unit = withMethdNameLogging { methodName =>
    try {
      val hiveDDL =
        s""" create database if not exists ${pcatProps.benchMarkTestHiveDB}
            |location "${pcatProps.benchMarkTestHiveLocation}"
         """.stripMargin

      sparkSession.sql(hiveDDL)

      stats += (s"$methodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("Hive DB" -> hiveDDL)
    }
    catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $methodName")
    }
  }


  /**
    * Drops the Hive Database in the End
    */
  def cleanUpHiveDB(): Unit = withMethdNameLogging { methodName =>
    try {
      sparkSession.sql(s"DROP DATABASE IF EXISTS ${pcatProps.benchMarkTestHiveDB} CASCADE")
      sparkSession.sql(s"dfs -rm -r -f ${pcatProps.benchMarkTestHiveLocation}")
      stats += (s"$methodName" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $methodName")
    }
  }

  /**
    * Prepare Dummy/Sample Data for BenchmarkTest
    *
    * @param numberOfRows Total Number of Sample Rows to Prep
    * @return DataFrame
    */
  def prepareBenchMarkTestData(numberOfRows: Int = 10): DataFrame = withMethdNameLogging { methodName =>
    try {
      def stringed(n: Int) = s"""{"id":$n,"time_created":"1500934200","time_processed":"1500934200","flags":"4325376","account_number":$n,"transaction_id":"NULL","counterparty":$n,"payee_email":"NULL","amount":"-545","status":"S","type":"U","reason":"NULL","time_received_payee":"NULL","time_created_payer":"NULL","memo":"NULL","payment_id":$n,"ach_id":"0","sync_group":"NULL","address_id":"0","payee_email_upper":"NULL","parent_id":"NULL","shared_id":$n,"cctrans_id":$n,"counterparty_alias":"NULL","counterparty_alias_type":"E","counterparty_alias_upper":"NULL","message":"NULL","time_user":"1500934200","message_id":$n,"subtype":"G","flags2":"268435712","time_inactive":"0","target_alias_id":$n,"counterparty_last_login_ip":"NULL","balance_at_time_created":"0","accept_deny_method":"NULL","currency_code":"USD","usd_amount":"-545","base_id":$n,"flags3":"163840","time_updated":"1500934200","transition":"A","flags4":"0","time_row_updated":"2017-07-24:15:10:04","flags5":$n,"db_ts_updated":"2017-07-24:22:10:04.000000000","db_ts_created":"2017-07-24:22:10:04.000000000","flags6":"128","flags7":"0"}"""

      val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }
      val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts, 50)
      val df: DataFrame = sparkSession.read.json(rdd)

      stats += (s"$methodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      df
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $methodName")
    }
  }


  /**
    * One Place to Handle Exception
    *
    * @param ex      Throwable Exception
    * @param message A Custom Message
    */
  def handleException(ex: Throwable, message: String = ""): Nothing = withMethdNameLogging { _ =>
    cleanUpHiveDB()
    ex.printStackTrace()
    throw new Exception(s"An Error Occurred <$message>")
  }

}





