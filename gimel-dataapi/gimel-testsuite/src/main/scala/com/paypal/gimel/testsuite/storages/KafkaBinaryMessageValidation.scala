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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

import com.paypal.gimel.DataSet
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.common.storageadmin.KafkaAdminClient
import com.paypal.gimel.kafka.conf.{KafkaConfigs, KafkaConstants}
import com.paypal.gimel.testsuite.utilities.GimelTestSuiteProperties

class KafkaBinaryMessageValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties) {

   info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestKafkaHiveTable}_1"
  val topicName = s"${gimelProps.smokeTestKafkaTopic}_1"

  /**
    * Creates Kafka Hive Table for Data API
    */
  private def bootStrapKafkaHive(): Unit = {
    def methodName: String = new Exception().getStackTrace().apply(1).getMethodName()
     info(" @Begin --> " + methodName)

    try {
      cleanUpKafkaHive()
      val hiveTableDDL =
        s"""
           |CREATE EXTERNAL TABLE $dataSetName(
           | `payload` binary
           |)
           |LOCATION '${gimelProps.smokeTestHiveLocation}/${gimelProps.smokeTestKafkaHiveTable}'
           |TBLPROPERTIES (
           |  '${GimelConstants.STORAGE_TYPE}'='kafka',
           |  '${KafkaConfigs.kafkaServerKey}'='${gimelProps.kafkaBroker}',
           |  '${KafkaConfigs.whiteListTopicsKey}'='$topicName',
           |  '${KafkaConfigs.serializerKey}'='${KafkaConfigs.kafkaStringSerializer}',
           |  '${KafkaConfigs.serializerValue}'='${KafkaConfigs.kafkaByteSerializer}',
           |  '${KafkaConfigs.deSerializerKey}'='${KafkaConfigs.kafkaStringDeSerializer}',
           |  '${KafkaConfigs.deSerializerValue}'='${KafkaConfigs.kafkaByteDeSerializer}',
           |  '${KafkaConfigs.zookeeperConnectionTimeoutKey}'='10000',
           |  '${KafkaConfigs.kafkaGroupIdKey}'='1',
           |  '${KafkaConfigs.offsetResetKey}'='earliest',
           |  '${KafkaConfigs.zookeeperCheckpointHost}'='${gimelProps.zkHostAndPort}',
           |  '${KafkaConfigs.zookeeperCheckpointPath}'='/pcatalog/kafka_consumer/checkpoint',
           |  '${KafkaConfigs.messageColumnAliasKey}'='value',
           |  '${KafkaConfigs.kafkaMessageValueType}'='binary'
           |)
         """.stripMargin

       info(s"DDLS -> $hiveTableDDL")
      sparkSession.sql(hiveTableDDL)

      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_CDH_kafka" -> hiveTableDDL)
    }
    catch {
      case ex: Throwable => handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Creates Kafka Topic
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapKafka(): (Map[String, String], Map[String, String]) = {
    def methodName: String = new Exception().getStackTrace().apply(1).getMethodName()
     info(" @Begin --> " + methodName)

    try {
      KafkaAdminClient.deleteTopicIfExists(
        gimelProps.zkHostAndPort
        , topicName
      )
      storageadmin.KafkaAdminClient.createTopicIfNotExists(
        gimelProps.zkHostAndPort
        , topicName
        , 1
        , 1
      )
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
    }
    catch {
      case ex: Throwable => handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
    (ddls, stats)
  }

  /**
    * BootStrap all the required Storage Objects
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def bootStrap(): (Map[String, String], Map[String, String]) = {
    def methodName: String = new Exception().getStackTrace().apply(1).getMethodName()
     info(" @Begin --> " + methodName)

    bootStrapKafka()
    bootStrapKafkaHive()
    (ddls, stats)
  }

  /**
    * CleanUp Kafka Storage Pointers
    *
    * @return (DDL, STATS) - both are Map[String,String]
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = {
    def methodName: String = new Exception().getStackTrace().apply(1).getMethodName()
     info(" @Begin --> " + methodName)

    cleanUpKafka()
    cleanUpKafkaHive()
    (ddls, stats)
  }

  /**
    * Drops Kafka Topic Creates for Smoke Test Purpose
    */
  private def cleanUpKafka() = {
    def methodName: String = new Exception().getStackTrace().apply(1).getMethodName()
     info(" @Begin --> " + methodName)

    try {
      storageadmin.KafkaAdminClient.deleteTopicIfExists(
        gimelProps.zkHostAndPort
        , topicName
      )

      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
    }
    catch {
      case ex: Throwable => handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Drops Kafka Hive Table Created to Test Data API - Read and Write
    */
  private def cleanUpKafkaHive() = {
    def methodName: String = new Exception().getStackTrace().apply(1).getMethodName()
     info(" @Begin --> " + methodName)

    try {
      val dropTableStatement = s"drop table if exists ${dataSetName}"
      sparkSession.sql(dropTableStatement)
      ddls += ("kafka_hive_ddl_drop" -> dropTableStatement)
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
    }
    catch {
      case ex: Throwable => handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Main Place to implement Validation Steps
    *
    * @param testData DataFrame (Optional)
    * @return @return A Tuple of (DDL , STATS, Optional[DataFrame])
    */
  override def validateAPI(testData: Option[DataFrame] = None): (Map[String, String], Map[String, String], Option[DataFrame]) = {
    def methodName: String = new Exception().getStackTrace().apply(1).getMethodName()
     info(" @Begin --> " + methodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"${methodName}-${storage}"
    try {

      val kBytes = (1 to gimelProps.smokeTestSampleRowsCount.toInt).map { x => s"value-${x}".getBytes() }
      val bytesRDD: RDD[Array[Byte]] = sparkSession.sparkContext.parallelize(kBytes)
      val testRow: RDD[Row] = bytesRDD.map(x => org.apache.spark.sql.Row(x))
      val field = StructType(Seq(StructField("value", BinaryType)))
      val testDF = sparkSession.createDataFrame(testRow, field)
      val dataSet = dataSetName
       info(s"${tag} | Begin Write to ${dataSet}...")
      dataset.write(dataSet, testDF)
       info(s"${tag} | Write Success.")
       info(s"${tag} | Read from ${dataSet}...")
      val readDF = dataset.read(dataSet)
      val count = readDF.count()
       info(s"${tag} | Read Count ${count}...")
       info(s"${tag} | Sample 10 Rows -->")
      readDF.show(10)
      compareDataFrames(testDF, readDF)
      stats += (s"${tag}" -> s"Success @ ${Calendar.getInstance.getTime}")
    }
    catch {
      case ex: Throwable => {
        stats += (s"${tag}" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method ${methodName}")
      }
    }
    (ddls, stats, testData)
  }

}
