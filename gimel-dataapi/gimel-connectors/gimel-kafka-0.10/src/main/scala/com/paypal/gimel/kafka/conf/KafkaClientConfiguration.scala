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

package com.paypal.gimel.kafka.conf

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.language.implicitConversions

import io.confluent.kafka.schemaregistry.client.rest.RestService

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{CatalogProviderConstants, GimelConstants, GimelProperties}
import com.paypal.gimel.common.schema.SchemaRegistryLookUp
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
  val hiveDBName = tableProps.getOrElse(CatalogProviderConstants.PROPS_NAMESPACE, GimelConstants.PCATALOG_STRING)
  val hiveTableName = tableProps(CatalogProviderConstants.DATASET_PROPS_DATASET)
  val clusterName = props.getOrElse(KafkaConstants.cluster, "unknown")

  logger.info(s"Hive Table Props --> ${tableProps.map(x => s"${x._1} --> ${x._2}").mkString("\n")}")

  // Schema Source either comes from Table "INLINE" (as a property) or from confluent Schema Registry if its = "CDH" or "CSR"
  val avroSchemaSource: String = tableProps.getOrElse(KafkaConfigs.avroSchemaSource, KafkaConstants.gimelKafkaAvroSchemaInline)
  val avroSchemaURL: String = tableProps.getOrElse(KafkaConfigs.avroSchemaSourceUrl, pcatProps.confluentSchemaURL)
  val avroSchemaWrapperKey: String = tableProps.getOrElse(KafkaConfigs.avroSchemaSourceWrapperKey, pcatProps.kafkaAvroSchemaKey)
  val avroSchemaKey: String = tableProps.getOrElse(KafkaConfigs.avroSchemaSourceKey, "")
  val (avroSchemaString, cdhTopicSchemaMetadata, cdhAllSchemaDetails) =
    avroSchemaSource.toUpperCase() match {
      case KafkaConstants.gimelKafkaAvroSchemaCDH =>
        val schemaRegistryClient = new RestService(avroSchemaURL)
        val allSchemas = SchemaRegistryLookUp.getAllSubjectAndSchema(avroSchemaURL)
        (schemaRegistryClient.getLatestVersion(avroSchemaWrapperKey).getSchema,
          Some(allSchemas(avroSchemaKey)._1),
          Some(allSchemas)
          )
      case KafkaConstants.gimeKafkaAvroSchemaCSR =>
        val schemaRegistryClient = new RestService(avroSchemaURL)
        (schemaRegistryClient.getLatestVersion(avroSchemaWrapperKey).getSchema,
          None,
          None
          )
      case KafkaConstants.gimelKafkaAvroSchemaInline =>
        (tableProps.getOrElse(KafkaConfigs.avroSchemaStringKey, ""), None, None)
      case _ =>
        throw new Exception(s"Unsupported Schema Source Supplied --> $avroSchemaSource")
    }

  // Kafka Props
  val randomId: String = scala.util.Random.nextInt.toString
  val kafkaHostsAndPort: String = tableProps.getOrElse(KafkaConfigs.kafkaServerKey, pcatProps.kafkaBroker)
  val KafkaConsumerGroupID: String = props.getOrElse(KafkaConfigs.kafkaGroupIdKey, tableProps.getOrElse(KafkaConfigs.kafkaGroupIdKey, randomId)).toString
  val kafkaConsumerID: String = props.getOrElse(KafkaConfigs.consumerId, tableProps.getOrElse(KafkaConfigs.consumerId, appTag)).toString.replaceAllLiterally("/", "_").replaceAllLiterally(":", "_")
  val kafkaZKTimeOutMilliSec: String = tableProps.getOrElse(KafkaConfigs.zookeeperConnectionTimeoutKey, 10000.toString)
  val kafkaAutoOffsetReset: String = tableProps.getOrElse(KafkaConfigs.offsetResetKey, "smallest")
  val kafkaCustomOffsetRange: String = tableProps.getOrElse(KafkaConfigs.customOffsetRange, "")
  val consumerModeBatch: String = tableProps.getOrElse(KafkaConstants.gimelAuditRunTypeBatch, "BATCH")
  val consumerModeStream: String = tableProps.getOrElse(KafkaConstants.gimelAuditRunTypeStream, "STREAM")
  val kafkaTopics: String = tableProps.getOrElse(KafkaConfigs.whiteListTopicsKey, "")

  // Kafka Serde
  val kafkaKeySerializer: String = tableProps.getOrElse(KafkaConfigs.serializerKey, KafkaConfigs.kafkaStringSerializer)
  val kafkaValueSerializer: String = tableProps.getOrElse(KafkaConfigs.serializerValue, KafkaConfigs.kafkaByteSerializer)
  val kafkaKeyDeSerializer: String = tableProps.getOrElse(KafkaConfigs.deSerializerKey, KafkaConfigs.kafkaStringDeSerializer)
  val kafkaValueDeSerializer: String = tableProps.getOrElse(KafkaConfigs.deSerializerValue, KafkaConfigs.kafkaByteDeSerializer)

  // Kafka Message Value Type --> String, Byte, Avro, JSON
  val kafkaMessageValueType: Option[String] = tableProps.get(KafkaConfigs.kafkaMessageValueType)

  // Zookeeper Details
  val zkHostAndPort: String = tableProps.getOrElse(KafkaConfigs.zookeeperCheckpointHost, pcatProps.zkHostAndPort)
  if (pcatProps.kafkaConsumerCheckPointRoot == "") throw new Exception("Root CheckPoint Path for ZK cannot be Empty")
  if (appTag == "") throw new Exception("appTag cannot be Empty")
  if (kafkaTopics == "") throw new Exception("kafkaTopics cannot be Empty")
  val zkCheckPoints: Seq[String] = kafkaTopics.split(",").map{ kafkaTopic =>
    tableProps.getOrElse(KafkaConfigs.zookeeperCheckpointPath, pcatProps.kafkaConsumerCheckPointRoot) + "/" + appTag + "/" + kafkaTopic
  }

  // Kafka Monitoring for PayPal
  /*
  val kafkaMetricsReporter = props.getOrElse(KafkaConfigs.paypalMetricsReporterKey, KafkaConfigs.paypalMetricsReporterValue).toString
  val kafkaMonitoringCluster = props.getOrElse(KafkaConfigs.paypalKafkaClusterKey, "unknown").toString
  val kafkaMonitoringColo = props.getOrElse(KafkaConfigs.paypalMonitoringColoKey, "unknown").toString
  val kafkaMonitoringPoolDefault = kafkaConsumerID
  val kafkaMonitoringPool = "Gimel-" + props.getOrElse(KafkaConfigs.paypalMonitoringPoolKey, kafkaMonitoringPoolDefault).toString
  val kafkaInterceptorClasses = props.getOrElse(KafkaConfigs.paypalInterceptorClassesKey, KafkaConfigs.paypalInterceptorClassName).toString
  val kafkaMetricsSamplingWindowMilliSec = props.getOrElse(KafkaConfigs.paypalMetricsSamplingMilliSecKey, "6000").toString
*/
  val clientProps = scala.collection.immutable.Map(
    KafkaConfigs.kafkaServerKey -> kafkaHostsAndPort
    , KafkaConfigs.kafkaGroupIdKey -> s"${KafkaConsumerGroupID}"
    , KafkaConfigs.kafkaClientIdKey -> s"${scala.util.Random.nextInt.toString}_${kafkaConsumerID}".takeRight(128)
  )

//  val ppKafkaListnerProps = scala.collection.immutable.Map(
//    KafkaConfigs.paypalMetricsReporterKey -> kafkaMetricsReporter
//    , KafkaConfigs.paypalKafkaClusterKey -> kafkaMonitoringCluster
//    , KafkaConfigs.paypalMonitoringColoKey -> kafkaMonitoringColo
//    , KafkaConfigs.paypalMonitoringPoolKey -> kafkaMonitoringPool
//    , KafkaConfigs.paypalInterceptorClassesKey -> kafkaInterceptorClasses
//    , KafkaConfigs.paypalMetricsSamplingMilliSecKey -> kafkaMetricsSamplingWindowMilliSec
//  )

  // Explicitly Making a Map of Properties that are necessary to Connect to Kafka for Subscribes (Reads)
  val kafkaConsumerProps: Map[String, String] = scala.collection.immutable.Map(KafkaConfigs.kafkaServerKey -> kafkaHostsAndPort
    , KafkaConfigs.kafkaGroupIdKey -> KafkaConsumerGroupID
    , KafkaConfigs.zookeeperConnectionTimeoutKey -> kafkaZKTimeOutMilliSec
    , KafkaConfigs.offsetResetKey -> kafkaAutoOffsetReset
    , KafkaConfigs.kafkaTopicKey -> kafkaTopics
    , KafkaConfigs.serializerKey -> kafkaKeySerializer
    , KafkaConfigs.serializerValue -> kafkaValueSerializer
    , KafkaConfigs.deSerializerKey -> kafkaKeyDeSerializer
    , KafkaConfigs.deSerializerValue -> kafkaValueDeSerializer
  ) ++ clientProps

  logger.info(s"KafkaConsumerProps --> ${kafkaConsumerProps.mkString("\n")}")

  // Explicitly Making a Map of Properties that are necessary to Connect to Kafka for Publishes (Writes)
  val kafkaProducerProps: Properties = new java.util.Properties()
  val producerProps = scala.collection.immutable.Map(KafkaConfigs.kafkaServerKey -> kafkaHostsAndPort
    , KafkaConfigs.serializerKey -> kafkaKeySerializer
    , KafkaConfigs.serializerValue -> kafkaValueSerializer
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
  val maxRatePerPartition: String = props.getOrElse(KafkaConfigs.maxRatePerPartitionKey, 3600).toString
  val streamParallelismFactor: Int = props.getOrElse(KafkaConfigs.streamParallelKey, 10).toString.toInt
  val isStreamParallel: Boolean = props.getOrElse(KafkaConfigs.isStreamParallelKey, "true").toString.toBoolean

  // Resolve fields for empty kafka topic property
  val fieldsBindToJSONString = tableProps.getOrElse(GimelConstants.FIELDS_BIND_TO_JSON, "")

  // Additional CDH Metadata Fields @todo this is not used in the code yet, KafkaUtilities implements this inside - this must superceed everywhere.
  val additionalCDHFields = scala.collection.Map("gg_commit_timestamp" -> "opTs", "opt_type" -> "opType", "trail_seq_no" -> "trailSeqno", "trail_rba" -> "trailRba")

  logger.info(s"Fields Initiated --> ${this.getClass.getFields.map(f => s"${f.getName} --> ${f.get().toString}").mkString("\n")}")
  logger.info(s"Completed Building --> ${this.getClass.getName}")

}

