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

package com.paypal.gimel.kafka2.conf

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.language.implicitConversions

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{GimelConstants, GimelProperties}
import com.paypal.gimel.logger.Logger

/**
  * Gimel Client Configuration for Kafka Dataset Operations.
  *
  * @param props Kafka Client properties.
  */
class KafkaClientConfiguration(val props: Map[String, Any]) {

  private val logger = Logger()
  logger.info(s"Begin Building --> ${this.getClass.getName}")
  //  logger.info(s"Incoming Properties --> ${props.map(x => s"${x._1} -> ${x._2}").mkString("\n")}")

  // Load Default Prop from Resource File
  val pcatProps = GimelProperties()

  // appTag is used to maintain checkpoints & various other factors that are unique to the application
  val appTag: String = props.getOrElse(GimelConstants.APP_TAG, "").toString

  // This is the DataSet Properties
  val datasetProps: DataSetProperties = props(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  val tableProps: Map[String, String] = datasetProps.props
  val clusterName = props.getOrElse(KafkaConstants.cluster, GimelConstants.UNKNOWN_STRING.toLowerCase)

  // Schema Source either comes from Table "INLINE" (as a property) or from confluent Schema Registry if its = "CSR"
  val avroSchemaSource: String = tableProps.getOrElse(KafkaConfigs.avroSchemaSource, "")
  val avroSchemaURL: String = tableProps.getOrElse(KafkaConfigs.avroSchemaSourceUrl, pcatProps.confluentSchemaURL)
  val avroSchemaWrapperKey: String = tableProps.getOrElse(KafkaConfigs.avroSchemaSourceWrapperKey, pcatProps.kafkaAvroSchemaKey)
  val avroSchemaKey: String = tableProps.getOrElse(KafkaConfigs.avroSchemaSourceKey, "")

  // Kafka Props
  val randomId: String = scala.util.Random.nextInt.toString
  val kafkaHostsAndPort: String = tableProps.getOrElse(KafkaConfigs.kafkaServerKey, pcatProps.kafkaBroker)
  val KafkaConsumerGroupID: String = props.getOrElse(KafkaConfigs.kafkaGroupIdKey, tableProps.getOrElse(KafkaConfigs.kafkaGroupIdKey, randomId)).toString
  val kafkaConsumerID: String = props.getOrElse(KafkaConfigs.consumerId, tableProps.getOrElse(KafkaConfigs.consumerId, appTag)).toString.replaceAllLiterally("/", "_").replaceAllLiterally(":", "_")
  val kafkaZKTimeOutMilliSec: String = tableProps.getOrElse(KafkaConfigs.zookeeperConnectionTimeoutKey, 10000.toString)
  val kafkaAutoOffsetReset: String = tableProps.getOrElse(KafkaConfigs.offsetResetKey, "latest")
  val kafkaTopics: String = tableProps.getOrElse(KafkaConfigs.whiteListTopicsKey, "")
  val kafkaCustomOffsetRange: String = tableProps.getOrElse(KafkaConfigs.customOffsetRange, "")
  val kafkaSourceFieldsListProp = tableProps.getOrElse(KafkaConfigs.kafkaSourceFieldsListKey, "value")
  val kafkaSourceFieldsList = if (kafkaSourceFieldsListProp.isEmpty) "value" else kafkaSourceFieldsListProp

  // Zookeeper Details
  val zkHostAndPort: String = tableProps.getOrElse(KafkaConfigs.zookeeperCheckpointHost, pcatProps.zkHostAndPort)
  if (pcatProps.kafkaConsumerCheckPointRoot == "") throw new Exception("Root CheckPoint Path for ZK cannot be Empty")
  if (appTag == "") throw new Exception("appTag cannot be Empty")
  if (kafkaTopics == "") throw new Exception("kafkaTopic cannot be Empty")
  val zkCheckPoints: Seq[String] = kafkaTopics.split(",").map { kafkaTopic =>
    tableProps.getOrElse(KafkaConfigs.zookeeperCheckpointPath, pcatProps.kafkaConsumerCheckPointRoot) + "/" + appTag + "/" + kafkaTopic
  }

  val clientProps = scala.collection.immutable.Map(
    KafkaConfigs.kafkaServerKey -> kafkaHostsAndPort
    , KafkaConfigs.kafkaGroupIdKey -> s"${KafkaConsumerGroupID}"
    , KafkaConfigs.kafkaClientIdKey -> s"${scala.util.Random.nextInt.toString}_${kafkaConsumerID}".takeRight(128)
  )

  // Explicitly Making a Map of Properties that are necessary to Connect to Kafka for Subscribes (Reads)
  val kafkaConsumerProps: Map[String, String] = scala.collection.immutable.Map(KafkaConfigs.kafkaServerKey -> kafkaHostsAndPort
    , KafkaConfigs.kafkaGroupIdKey -> KafkaConsumerGroupID
    , KafkaConfigs.zookeeperConnectionTimeoutKey -> kafkaZKTimeOutMilliSec
    , KafkaConfigs.offsetResetKey -> kafkaAutoOffsetReset
    , KafkaConfigs.kafkaTopicKey -> kafkaTopics
  ) ++ clientProps

  logger.info(s"KafkaConsumerProps --> ${kafkaConsumerProps.mkString("\n")}")

  // Explicitly Making a Map of Properties that are necessary to Connect to Kafka for Publishes (Writes)
  val kafkaProducerProps: Properties = new java.util.Properties()
  val producerProps = scala.collection.immutable.Map(KafkaConfigs.kafkaServerKey -> kafkaHostsAndPort
    , KafkaConfigs.kafkaTopicKey -> kafkaTopics)
  producerProps.foreach { kvPair => kafkaProducerProps.put(kvPair._1.toString, kvPair._2.toString) }

  logger.info(s"kafkaProducerProps --> ${kafkaProducerProps.asScala.mkString("\n")}")

  // These are key throttling factors for Improved Performance in Batch Mode
  val maxRecsPerPartition: Long = props.getOrElse(KafkaConfigs.maxRecordsPerPartition, 2500000).toString.toLong
  val parallelsPerPartition: Int = props.getOrElse(KafkaConfigs.batchFetchSizeTemp, 250).toString.toInt
  val minRowsPerParallel: Long = props.getOrElse(KafkaConfigs.minRowsPerParallelKey, 100000).toString.toLong
  val fetchRowsOnFirstRun: Long = props.getOrElse(KafkaConfigs.rowCountOnFirstRunKey, 2500000).toString.toLong
  val targetCoalesceFactor: Int = props.getOrElse(KafkaConfigs.targetCoalesceFactorKey, 1).toString.toInt

  // These are key throttling factors for Improved Performance in Streaming Mode
  val maxRatePerTrigger: String = props.getOrElse(KafkaConfigs.maxRatePerTriggerKey, "").toString
  val streamParallelismFactor: Int = props.getOrElse(KafkaConfigs.streamParallelKey, 10).toString.toInt
  val isStreamParallel: Boolean = props.getOrElse(KafkaConfigs.isStreamParallelKey, "false").toString.toBoolean

  // Fields bind to option for empty topic
  val fieldsBindToJSONString = tableProps.getOrElse(GimelConstants.FIELDS_BIND_TO_JSON, "")

  // Structured Streaming
  val streamingCheckpointLocation = tableProps.getOrElse(GimelConstants.GIMEL_STREAMING_CHECKPOINT_LOCATION,
    props.getOrElse(GimelConstants.GIMEL_STREAMING_CHECKPOINT_LOCATION, "")).toString
  val streamingOutputMode = tableProps.getOrElse(GimelConstants.GIMEL_STREAMING_OUTPUT_MODE, "append")
  val failOnDataLoss = tableProps.getOrElse(KafkaConstants.STREAM_FAIL_ON_DATA_LOSS, "false")

  logger.info(s"Fields Initiated --> ${this.getClass.getFields.map(f => s"${f.getName} --> ${f.get().toString}").mkString("\n")}")
  logger.info(s"Completed Building --> ${this.getClass.getName}")

}

