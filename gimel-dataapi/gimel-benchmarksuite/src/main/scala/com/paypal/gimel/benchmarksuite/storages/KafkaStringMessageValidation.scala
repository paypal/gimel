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

package com.paypal.gimel.benchmarksuite.storages

import java.util.Calendar

import scala.collection.immutable.Map

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.spark.streaming.kafka010.OffsetRange

import com.paypal.gimel.DataSet
import com.paypal.gimel.benchmarksuite.utilities.GimelBenchmarkProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.common.storageadmin.KafkaAdminUtils
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.kafka.conf.KafkaConfigs
import com.paypal.gimel.kafka.utilities.BrokersAndTopic
import com.paypal.gimel.kafka.utilities.ImplicitKafkaConverters._


class KafkaStringMessageValidation(dataset: DataSet, sparkSession: SparkSession, sqlContext: SQLContext, pcatProps: GimelBenchmarkProperties, testData: DataFrame)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, pcatProps: GimelBenchmarkProperties, testData: DataFrame) {

  logger.info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestKafkaTopic_Dataset}_1"
  val nativeName = s"${pcatProps.benchMarkTestKafkaTopic_Native}_1"
  val topicName_native = s"${pcatProps.benchMarkTestKafkaTopic_Native}_1"
  val topicName_dataset = s"${pcatProps.benchMarkTestKafkaTopic_Dataset}_1"

  /**
    * Creates Kafka Hive Table for Data API
    */
  private def bootStrapKafkaHive(): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      cleanUpKafkaHive()
      val hiveTableDDL =
        s"""
           |CREATE EXTERNAL TABLE `$dataSetName`(
           | `payload` string
           |)
           |TBLPROPERTIES (
           |  '${KafkaConfigs.kafkaServerKey}'='${pcatProps.kafkaBroker}',
           |  '${KafkaConfigs.whiteListTopicsKey}'='$topicName_dataset',
           |  '${KafkaConfigs.serializerKey}'='${KafkaConfigs.kafkaStringSerializer}',
           |  '${KafkaConfigs.serializerValue}'='${KafkaConfigs.kafkaStringSerializer}',
           |  '${KafkaConfigs.zookeeperConnectionTimeoutKey}'='10000',
           |  '${KafkaConfigs.kafkaGroupIdKey}'='1',
           |  '${KafkaConfigs.offsetResetKey}'='smallest',
           |  '${GimelConstants.STORAGE_TYPE}'='kafka',
           |  '${KafkaConfigs.zookeeperCheckpointHost}'='${pcatProps.zkHostAndPort}',
           |  '${KafkaConfigs.zookeeperCheckpointPath}'='/pcatalog/kafka_consumer/checkpoint',
           |  '${KafkaConfigs.messageColumnAliasKey}'='value_message'
           |)
         """.stripMargin

      logger.info(s"DDLS -> $hiveTableDDL")
      sparkSession.sql(hiveTableDDL)

      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_CDH_kafka" -> hiveTableDDL)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
  }

  /**
    * Creates Kafka Topic
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapKafka() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      KafkaAdminUtils.deleteTopicIfExists(
        pcatProps.zkHostAndPort
        , topicName_native
      )
      storageadmin.KafkaAdminUtils.createTopicIfNotExists(
        pcatProps.zkHostAndPort
        , topicName_native
        , 1
        , 1
      )
      KafkaAdminUtils.deleteTopicIfExists(
        pcatProps.zkHostAndPort
        , topicName_dataset
      )
      storageadmin.KafkaAdminUtils.createTopicIfNotExists(
        pcatProps.zkHostAndPort
        , topicName_dataset
        , 1
        , 1
      )
      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
    (ddls, stats)
  }

  /**
    * BootStrap all the required Storage Objects
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def bootStrap(): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    bootStrapKafka()
    bootStrapKafkaHive()
    (ddls, stats)
  }

  /**
    * CleanUp Kafka Storage Pointers
    *
    * @return (DDL, STATS) - both are Map[String, String]
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    cleanUpKafka()
    cleanUpKafkaHive()
    (ddls, stats)
  }


  /**
    * Benchmark Kafka String Message
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def benchmark(): (Map[String, String], Map[String, String]) = {
    benchmarkKafkaStringMessage()
  }

  /**
    * Benchmark Kafka String Message -Both Native and Dataset
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def benchmarkKafkaStringMessage() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"${MethodName}-${storage}"

    try {
      val testDataCount = testData.count()
      val testDataOption = Some(testData)
      val dataSet = dataSetName
      val nativeAPISet = nativeName
      logger.info(s"${tag} | TestDataCount ${testDataCount}")
      logger.info(s"${tag} | Begin Bench Mark Test..")
      logger.info(s"${tag} | Begin Bench Mark Native API to ${nativeAPISet}...")
      benchmarkNativeKafkaStringMessageAPI(testDataOption)
      logger.info(s"${tag} | End Bench Mark Native API to ${nativeAPISet}...")
      logger.info(s"${tag} | Begin Bench Mark Dataset API to ${dataSet}...")
      benchmarkDatasetKafkaStringMessageAPI(testDataOption)
      logger.info(s"${tag} | End Bench Mark Dataset API to ${dataSet}...")
    } catch {
      case ex: Throwable =>
        stats += (s"${tag}" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
    (ddls, stats)
  }


  /**
    * Drops Kafka Topic Creates for Benchmark Test Purpose
    */
  private def cleanUpKafka() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      storageadmin.KafkaAdminUtils.deleteTopicIfExists(
        pcatProps.zkHostAndPort
        , topicName_native
      )
      storageadmin.KafkaAdminUtils.deleteTopicIfExists(
        pcatProps.zkHostAndPort
        , topicName_dataset
      )

      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
  }

  /**
    * Drops Kafka Hive Table Created to Test Data API - Read and Write
    */
  private def cleanUpKafkaHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val dropTableStatement = s"drop table if exists ${dataSetName}"
      sparkSession.sql(dropTableStatement)
      ddls += ("kafka_hive_ddl_drop" -> dropTableStatement)
      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
  }

  /**
    * BenchMark Kafka String Native
    *
    * @param testDataDF DataFrame (Optional)
    * @return @return A Tuple of (DDL , STATS, Optional[DataFrame])
    */
  def benchmarkNativeKafkaStringMessageAPI(testDataDF: Option[DataFrame] = None): (Map[String, String], Map[String, String], DataFrame) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    var resultsAPIData: Map[String, String] = Map()
    resultsAPIData += (s"type" -> s"Native")
    resultsAPIData += (s"StorageType" -> s"Kafka_String")

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"${MethodName}-${storage}"

    try {
      val testData: RDD[String] = testDataDF.get.toJSON.rdd
      val testRow: RDD[Row] = testData.map(x => org.apache.spark.sql.Row(x))
      val field = StructType(Seq(StructField("value_message", StringType)))
      val testDF = sparkSession.createDataFrame(testRow, field)
      val kafkaProps = pcatProps.kafkaProducerProps
      val kafkaNativeTopic = topicName_native
      val datasetReadTimer = Timer()
      val datasetWriteTimer = Timer()
      val dataSet = nativeName
      logger.info(s"${tag} | Begin Write to ${dataSet}...")
      datasetWriteTimer.start
      testData.foreachPartition { eachPartition =>
        val producer: KafkaProducer[Nothing, String] = new KafkaProducer(kafkaProps)
        val resp = eachPartition.map { messageString =>
          val rec = new ProducerRecord(kafkaNativeTopic, messageString)
          producer.send(rec)
        }
        resp.length
        producer.close()
      }
      logger.info("Publish to Kafka - Completed !")
      val datasetWriteTimeValue = datasetWriteTimer.endWithMillSecRunTime / 1000
      resultsAPIData += (s"writeTime" -> s"${datasetWriteTimeValue}")
      logger.info(s"${tag} | Write Success.")


      logger.info(s"${tag} | Read from ${dataSet}...")
      val kafkaConsumerProps: Map[String, String] = scala.collection.immutable.Map(KafkaConfigs.kafkaServerKey -> pcatProps.kafkaBroker
        , KafkaConfigs.kafkaGroupIdKey -> pcatProps.KafkaConsumerGroupID
        , KafkaConfigs.zookeeperConnectionTimeoutKey -> pcatProps.kafkaZKTimeOutMilliSec
        , KafkaConfigs.offsetResetKey -> pcatProps.kafkaAutoOffsetReset
        , KafkaConfigs.kafkaTopicKey -> topicName_native)
      datasetReadTimer.start
      val availableOffsetRange: Array[OffsetRange] = BrokersAndTopic(pcatProps.kafkaBroker, topicName_native).toKafkaOffsetsPerPartition
      // val newOffsetRangesForReader = getNewOffsetRangeForReader(lastCheckPoint, availableOffsetRange, 1000000)
      val kafkaProps1: java.util.Map[String, Object] = null
      kafkaConsumerProps.map(x => kafkaProps1.put(x._1, x._2))
      val rdd: RDD[String] = createRDD[String, String](
        sqlContext.sparkContext, kafkaProps1, availableOffsetRange, LocationStrategies.PreferConsistent).map(x => x.value())
      val readRow: RDD[Row] = rdd.map(x => org.apache.spark.sql.Row(x))
      val field_value = StructType(Seq(StructField("value_message", StringType)))
      val readDF = sparkSession.createDataFrame(readRow, field_value).cache()
      val count = readDF.count()
      val datasetReadTimeValue = datasetReadTimer.endWithMillSecRunTime / 1000
      resultsAPIData += (s"readTime" -> s"${datasetReadTimeValue}")
      logger.info(s"${tag} | Read Count ${count}...")
      logger.info(s"${tag} | Sample 10 Rows -->")
      readDF.show(10)
      compareDataFrames(testDF, readDF)
      stats += (s"${tag}" -> s"Success @ ${Calendar.getInstance.getTime}")
      bootStrapESIndexForStats()
      postResults(resultsAPIData)

    } catch {
      case ex: Throwable =>
        stats += (s"${tag}" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
    (ddls, stats, testData)
  }


  /**
    * BenchMark Kafka String
    *
    * @param testDataDF DataFrame (Optional)
    * @return @return A Tuple of (DDL , STATS, Optional[DataFrame])
    */
  def benchmarkDatasetKafkaStringMessageAPI(testDataDF: Option[DataFrame] = None): (Map[String, String], Map[String, String], DataFrame) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    var resultsAPIData: Map[String, String] = Map()
    resultsAPIData += (s"type" -> s"Dataset")
    resultsAPIData += (s"StorageType" -> s"Kafka_String")

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"${MethodName}-${storage}"

    try {
      val testData: RDD[String] = testDataDF.get.toJSON.rdd
      val testRow: RDD[Row] = testData.map(x => org.apache.spark.sql.Row(x))
      val field = StructType(Seq(StructField("value_message", StringType)))
      val testDF = sparkSession.createDataFrame(testRow, field)
      val datasetReadTimer = Timer()
      val datasetWriteTimer = Timer()
      val dataSet = dataSetName
      logger.info(s"${tag} | Begin Write to ${dataSet}...")
      datasetWriteTimer.start
      dataset.write(dataSet, testData, Map[String, Any]())
      val datasetWriteTimeValue = datasetWriteTimer.endWithMillSecRunTime / 1000
      resultsAPIData += (s"writeTime" -> s"${datasetWriteTimeValue}")
      logger.info(s"${tag} | Write Success.")
      logger.info(s"${tag} | Read from ${dataSet}...")
      datasetReadTimer.start
      val readDF = dataset.read(dataSet).cache()
      val count = readDF.count()
      val datasetReadTimeValue = datasetReadTimer.endWithMillSecRunTime / 1000
      resultsAPIData += (s"readTime" -> s"${datasetReadTimeValue}")
      logger.info(s"${tag} | Read Count ${count}...")
      logger.info(s"${tag} | Sample 10 Rows -->")
      readDF.show(10)
      compareDataFrames(testDF, readDF)
      stats += (s"${tag}" -> s"Success @ ${Calendar.getInstance.getTime}")
      bootStrapESIndexForStats()
      postResults(resultsAPIData)
    } catch {
      case ex: Throwable =>
        stats += (s"${tag}" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
    (ddls, stats, testData)
  }

}
