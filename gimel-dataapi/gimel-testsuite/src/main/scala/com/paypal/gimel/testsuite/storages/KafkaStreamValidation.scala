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

package com.paypal.gimel.testsuite.storages

import java.util.Calendar

import scala.language.implicitConversions

import org.apache.spark.sql._

import com.paypal.gimel.DataSet
import com.paypal.gimel.common.conf._
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.common.storageadmin.KafkaAdminUtils
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs
import com.paypal.gimel.kafka.conf.{KafkaConfigs, KafkaConstants}
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.testsuite.utilities.GimelTestSuiteProperties

class KafkaStreamValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties) {

  logger.info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${gimelProps.smokeTestHiveDB}.kafka_cdh_${gimelProps.smokeTestCDHKafkaStreamHiveTable}"
  val dataSetNameES = s"${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestKafkaStreamESHiveTable}"
  val topicName = s"${gimelProps.smokeTestKafkaTopic}_1"

  /**
    * Creates Hive Table required for Reading CDH, ES Hive table creation for writing to ES
    *
    * @return A Tuple of (DDL , STATS)
    */

  override def bootStrap(): (Map[String, String], Map[String, String]) = {
    bootStrapKafka()
    bootStrapStreamKafkaHive()
    bootstrapStreamESHive()
  }


  /**
    * Creates Kafka Topic
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapKafka() = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    try {
      KafkaAdminUtils.deleteTopicIfExists(
        gimelProps.zkHostAndPort
        , topicName
      )
      storageadmin.KafkaAdminUtils.createTopicIfNotExists(
        gimelProps.zkHostAndPort
        , topicName
        , 1
        , 1
      )
      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
    }
    catch {
      case ex: Throwable => handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
    (ddls, stats)
  }

  /**
    * Creates Hive Table required for Reading CDH
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapStreamKafkaHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    cleanUpKafkaHive()
    try {
      val topicName = s"${gimelProps.smokeTestKafkaTopic}_1"
      val kafkaDDL =
        s"""
           |CREATE EXTERNAL TABLE $dataSetName(
           | `payload` string
           |)
           |LOCATION '${gimelProps.smokeTestHiveLocation}/${gimelProps.smokeTestKafkaHiveTable}'
           |TBLPROPERTIES (
           |  '${GimelConstants.STORAGE_TYPE}'='KAFKA',
           |  '${KafkaConfigs.kafkaServerKey}'='${gimelProps.kafkaBroker}',
           |  '${KafkaConfigs.whiteListTopicsKey}'='$topicName',
           |  '${KafkaConfigs.serializerKey}'='${KafkaConfigs.kafkaStringSerializer}',
           |  '${KafkaConfigs.serializerValue}'='${KafkaConfigs.kafkaStringSerializer}',
           |  '${KafkaConfigs.deSerializerKey}'='${KafkaConfigs.kafkaStringDeSerializer}',
           |  '${KafkaConfigs.deSerializerValue}'='${KafkaConfigs.kafkaStringDeSerializer}',
           |  '${KafkaConfigs.zookeeperConnectionTimeoutKey}'='10000',
           |  '${KafkaConfigs.kafkaGroupIdKey}'='1',
           |  '${KafkaConfigs.offsetResetKey}'='earliest',
           |  '${KafkaConfigs.zookeeperCheckpointHost}'='${gimelProps.zkHostAndPort}',
           |  '${KafkaConfigs.zookeeperCheckpointPath}'='/pcatalog/kafka_consumer/checkpoint',
           |  '${KafkaConfigs.messageColumnAliasKey}'='value_message',
           |  '${KafkaConfigs.kafkaMessageValueType}'='json'
           |)
         """.stripMargin
      deployDDL(kafkaDDL)
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_CDH_Hive_Table_Streaming" -> kafkaDDL)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
    (ddls, stats)
  }

  /**
    * Drops Kafka Hive Table Created to Test Data API - Read and Write
    */
  private def cleanUpKafkaHive() = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    try {
      val dropTableStatement = s"drop table if exists ${dataSetName}"
      sparkSession.sql(dropTableStatement)
      ddls += ("kafka_hive_ddl_drop" -> dropTableStatement)
      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
    }
    catch {
      case ex: Throwable => handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
  }

  /**
    * ES Hive table creation for writing to ES
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootstrapStreamESHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val typeName = gimelProps.tagToAdd.replace("_", "")
      val esDDL =
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS $dataSetNameES
           |(
           |  `data` string COMMENT 'from deserializer'
           |)
           |LOCATION
           |  '${gimelProps.smokeTestHiveLocation}/${gimelProps.smokeTestKafkaStreamESIndex}'
           |TBLPROPERTIES (
           |  '${GimelConstants.STORAGE_TYPE}'='ELASTIC_SEARCH',
           |  '${ElasticSearchConfigs.esIndexAutoCreate}'='true',
           |  '${GimelConstants.ES_NODE}'='${gimelProps.esHost}',
           |  '${GimelConstants.ES_PORT}'='${gimelProps.esPort}',
           |  '${ElasticSearchConfigs.esResource}'='${gimelProps.smokeTestKafkaStreamESIndex}/$typeName'
           |)
      """.stripMargin

      // we are adding these jars because Hive Session needs these jar for executing the above DDL(It needs elasticsearch-hadoop jar for ESStorage Handler)
      // we are not using sparkSession.sql because spark 2.1 version doesnt support Stored by ES Storage Handler and Serde.so we are replacing with Hive JDBC as it supports both versions(1.6 and 2.1)
      deployDDL(esDDL)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_Kafka_stream_es" -> esDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }


  /**
    * Validate We are Able to Read From Kafka via Data API
    *
    * @param inDF The input data frame.
    * @return @return A Tuple of (DDL , STATS, Optional[DataFrame])
    */
  override def validateAPI(inDF: Option[DataFrame]): (Map[String, String], Map[String, String], Option[DataFrame]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"$MethodName-$storage"
    try {
      sparkSession.sql(s"set gimel.kafka.throttle.streaming.window.seconds=${gimelProps.smokeTestKafkaStreamBatchInterval}")
      sparkSession.sql(s"set gimel.kafka.throttle.batch.fetchRowsOnFirstRun=250")
      sparkSession.sql("set gimel.kafka.reader.checkpoint.clear=true")
      sparkSession.sql("set gimel.kafka.reader.checkpoint.save=false")
      sparkSession.sql(s"set ${ElasticSearchConfigs.esDefaultReadForAllPartitions}=true")
      sparkSession.sql(s"set gimel.kafka.streaming.awaitTerminationOrTimeout=${gimelProps.smokeTestStreamingAwaitTermination}")
      Logger(this.getClass.getName).info(s"Insert into $dataSetNameES select * from $dataSetName")
      val res = com.paypal.gimel.sql.GimelQueryProcessor.executeStream(
        s"Insert into $dataSetNameES select * from $dataSetName"
        , sparkSession
      )
      Logger(this.getClass.getName).info(res)
      stats += (s"$tag" -> s"Success @ ${Calendar.getInstance.getTime}")
      (ddls, stats, inDF)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * CleanUp the Kafka's, ES Hive Table and ES Index
    *
    * @return A Tuple of (DDL , STATS)
    */

  override def cleanUp(): (Map[String, String], Map[String, String]) = {
    kafkaStreamCleanUp()
    kafkaStreamESHiveCleanUp()
    val url = s"${gimelProps.esHost}:${gimelProps.esPort}/${gimelProps.smokeTestKafkaStreamESIndex}"
    kafkaStreamESCleanUp(url)
  }


  /**
    * CleanUp the Kafka's Hive Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def kafkaStreamCleanUp() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val dropDDL = s"drop table if exists $dataSetName"
      sparkSession.sql(dropDDL)
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("CDH_Hive_Table__Kafka_Stream_Drop" -> dropDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * CleanUp ES Hive Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def kafkaStreamESHiveCleanUp() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val dropDDL = s"drop table if exists $dataSetNameES"
      sparkSession.sql(dropDDL)
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("Kafka_Stream_ES_Drop_Table" -> dropDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Delete ES Index
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def kafkaStreamESCleanUp(url: String) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.info("delete index")
    logger.info(url)
    try {
      val output = storageadmin.ESAdminClient.deleteIndex(url)
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("Kafka_Stream_ES_Index Dropped_Status" -> output)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

}
