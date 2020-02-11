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

import org.apache.spark.sql._

import com.paypal.gimel.DataSet
import com.paypal.gimel.benchmarksuite.utilities.GimelBenchmarkProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.common.storageadmin.KafkaAdminUtils
import com.paypal.gimel.kafka.conf.KafkaConfigs

class KafkaAvroMessageValidation(dataset: DataSet, sparkSession: SparkSession, sqlContext: SQLContext, pcatProps: GimelBenchmarkProperties, testData: DataFrame)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, pcatProps: GimelBenchmarkProperties, testData: DataFrame) {

  logger.info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestKafkaTopic_Dataset}_2"
  val nativeName = ""
  val topicName_dataset = s"${pcatProps.benchMarkTestKafkaTopic_Dataset}_2"

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
           |CREATE EXTERNAL TABLE IF NOT EXISTS `$dataSetName`
           |(
           | `id` bigint,
           | `accept_deny_method` string,
           |`account_number` bigint,
           |`ach_id` string,
           |`address_id` string,
           |`amount` string,
           |`balance_at_time_created` string,
           |`base_id` bigint,
           |`cctrans_id` bigint,
           |`counterparty` bigint,
           |`counterparty_alias` string,
           |`counterparty_alias_type` string,
           |`counterparty_alias_upper` string,
           |`counterparty_last_login_ip` string,
           |`currency_code` string,
           |`db_ts_created` string,
           |`db_ts_updated` string,
           |`flags` string,
           |`flags2` string,
           |`flags3` string,
           |`flags4` string,
           |`flags5` bigint,
           |`flags6` string,
           |`flags7` string,
           |`memo` string,
           |`message` string,
           |`message_id` bigint,
           |`parent_id` string,
           |`payee_email` string,
           |`payee_email_upper` string,
           |`payment_id` bigint,
           |`reason` string,
           |`shared_id` bigint,
           |`status` string,
           |`subtype` string,
           |`sync_group` string,
           |`target_alias_id` bigint,
           |`time_created` string,
           |`time_created_payer` string,
           |`time_inactive` string,
           |`time_processed`string,
           |`time_received_payee` string,
           |`time_row_updated` string,
           |`time_updated` string,
           |`time_user` string,
           |`transaction_id` string,
           |`transition` string,
           |`type` string,
           |`usd_amount` string
           |)
           | LOCATION '${pcatProps.benchMarkTestHiveLocation}/${pcatProps.benchMarkTestKafkaTopic_Dataset}_2'
           | TBLPROPERTIES (
           |'${KafkaConfigs.kafkaServerKey}'='${pcatProps.kafkaBroker}',
           |'${KafkaConfigs.whiteListTopicsKey}'='$topicName_dataset',
           |'${KafkaConfigs.serializerKey}'='${KafkaConfigs.kafkaStringSerializer}',
           |'${KafkaConfigs.serializerValue}'='${KafkaConfigs.kafkaByteSerializer}',
           |'${KafkaConfigs.zookeeperConnectionTimeoutKey}'='10000',
           |'${KafkaConfigs.kafkaGroupIdKey}'='1',
           |'${GimelConstants.STORAGE_TYPE}'='kafka',
           |'${KafkaConfigs.offsetResetKey}'='smallest',
           |'${KafkaConfigs.avroSchemaStringKey}'=' {
           | "type" : "record",
           | "namespace" : "default",
           | "name" : "user",
           | "fields" : [
           | {"name": "id" , "type":"long" },
           | {"name":"accept_deny_method" , "type":"string"},
           | {"name":"account_number", "type":"long"},
           | {"name":"ach_id", "type":"string"},
           | {"name":"address_id", "type":"string"},
           | {"name":"amount","type":"string"},
           | {"name":"balance_at_time_created", "type":"string"},
           | {"name":"base_id","type":"long"},
           | {"name":"cctrans_id", "type":"long"},
           | {"name":"counterparty", "type":"long"},
           | {"name":"counterparty_alias", "type":"string"},
           | {"name":"counterparty_alias_type", "type":"string"},
           | {"name":"counterparty_alias_upper", "type":"string"},
           | {"name":"counterparty_last_login_ip", "type":"string"},
           | {"name":"currency_code", "type":"string"},
           | {"name":"db_ts_created", "type":"string"},
           | {"name":"db_ts_updated", "type":"string"},
           | {"name":"flags", "type":"string"},
           | {"name":"flags2", "type":"string"},
           | {"name":"flags3", "type":"string"},
           | {"name":"flags4", "type":"string"},
           | {"name":"flags5", "type":"long"},
           | {"name":"flags6", "type":"string"},
           | {"name":"flags7", "type":"string"},
           | {"name":"memo", "type":"string"},
           | {"name":"message", "type":"string"},
           | {"name":"message_id", "type":"long"},
           | {"name":"parent_id", "type":"string"},
           | {"name":"payee_email", "type":"string"},
           | {"name":"payee_email_upper", "type":"string"},
           | {"name":"payment_id", "type":"long"},
           | {"name":"reason", "type":"string"},
           | {"name":"shared_id", "type":"long"},
           | {"name":"status", "type":"string"},
           | {"name":"subtype", "type":"string"},
           | {"name":"sync_group", "type":"string"},
           | {"name":"target_alias_id", "type":"long"},
           | {"name":"time_created", "type":"string"},
           | {"name":"time_created_payer", "type":"string"},
           | {"name":"time_inactive", "type":"string"},
           | {"name":"time_processed","type":"string"},
           | {"name":"time_received_payee", "type":"string"},
           | {"name":"time_row_updated", "type":"string"},
           | {"name":"time_updated", "type":"string"},
           | {"name":"time_user", "type":"string"},
           | {"name":"transaction_id", "type":"string"},
           | {"name":"transition", "type":"string"},
           | {"name":"type", "type":"string"},
           | {"name":"usd_amount", "type":"string"}
           | ]}'
           | )
      """.stripMargin

      logger.info(s"DDLS -> $hiveTableDDL")
      sparkSession.sql(hiveTableDDL)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("kafka_avro_DDL" -> hiveTableDDL)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
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
        , topicName_dataset
      )
      storageadmin.KafkaAdminUtils.createTopicIfNotExists(
        pcatProps.zkHostAndPort
        , topicName_dataset
        , 1
        , 1
      )
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
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
    * Benchmark KAFKA Avro
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def benchmark(): (Map[String, String], Map[String, String]) = {
    benchmarkKafkaAvro()
  }


  /**
    * Benchmark  KAFKA Avro using Dataset
    *
    * @return A Tuple of (DDL , STATS)
    */


  private def benchmarkKafkaAvro() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"$MethodName-$storage"

    try {
      val testDataCount = testData.count()
      val testDataOption = Some(testData)
      val dataSet = dataSetName
      logger.info(s"$tag | TestDataCount $testDataCount")
      logger.info(s"$tag | Begin Bench Mark Test..")
      logger.info(s"$tag | Begin Bench Mark Dataset API to $dataSet...")
      benchmarkDatasetAPI(testDataOption, "Kafka_Avro")
      logger.info(s"$tag | End Bench Mark Dataset API to $dataSet...")
    } catch {
      case ex: Throwable =>
        stats += (s"$tag" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
    (ddls, stats)
  }


  /**
    * Drops Kafka Topic Creates
    */
  private def cleanUpKafka() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      storageadmin.KafkaAdminUtils.deleteTopicIfExists(
        pcatProps.zkHostAndPort
        , topicName_dataset
      )

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Drops Kafka Hive Table
    */
  private def cleanUpKafkaHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val dropTableStatement = s"drop table if exists $dataSetName"
      sparkSession.sql(dropTableStatement)
      ddls += ("kafka_hive_ddl_drop" -> dropTableStatement)
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

}
