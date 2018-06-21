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

object KafkaConfigs {

  // kafka properties
  val kafkaServerKey: String = "bootstrap.servers"
  val kafkaGroupIdKey: String = "group.id"
  val kafkaClientIdKey: String = "client.id"
  val paypalMetricsReporterKey: String = "metric.reporters"
  val paypalKafkaClusterKey: String = "kafka.monitoring.cluster"
  val paypalMonitoringColoKey: String = "kafka.monitoring.colo"
  val paypalMonitoringPoolKey: String = "kafka.monitoring.pool"
  val paypalInterceptorClassesKey: String = "interceptor.classes"
  val paypalMetricsSamplingMilliSecKey: String = "metrics.sample.window.ms"
  val zookeeperConnectionTimeoutKey: String = "zookeeper.connection.timeout.ms"
  val offsetResetKey: String = "auto.offset.reset"
  val kafkaTopicKey: String = "kafka.topic"
  val serializerKey: String = "key.serializer"
  val serializerValue: String = "value.serializer"
  val deSerializerKey: String = "key.deserializer"
  val deSerializerValue: String = "value.deserializer"
  val consumerId: String = "consumer.id"
  // misc properties for read/write
  val rowCountOnFirstRunKey: String = s"gimel.kafka.throttle.batch.fetchRowsOnFirstRun"
  val targetCoalesceFactorKey: String = "gimel.kafka.throttle.batch.targetCoalesceFactor"
  val minRowsPerParallelKey: String = s"gimel.kafka.throttle.batch.minRowsPerParallel"
  val batchFetchSize: String = s"gimel.kafka.throttle.batch.parallelsPerPartition"
  val maxRecordsPerPartition: String = s"gimel.kafka.throttle.batch.maxRecordsPerPartition"
  val batchFetchSizeTemp: String = s"gimel.kafka.throttle.batch.parallelsPerPartition"
  val messageColumnAliasKey: String = "gimel.kafka.message.column.alias"
  val avroSchemaStringKey: String = "gimel.kafka.avro.schema.string"
  val kafkaMessageValueType: String = "gimel.kafka.message.value.type"
  // metastore properties
  val zookeeperCheckpointHost: String = "gimel.kafka.checkpoint.zookeeper.host"
  val zookeeperCheckpointPath: String = "gimel.kafka.checkpoint.zookeeper.path"
  val avroSchemaSource: String = "gimel.kafka.avro.schema.source"
  val avroSchemaSourceUrl: String = s"${avroSchemaSource}.url"
  val avroSchemaSourceWrapperKey: String = s"${avroSchemaSource}.wrapper.key"
  val avroSchemaSourceKey: String = s"${avroSchemaSource}.key"
  val whiteListTopicsKey: String = "gimel.kafka.whitelist.topics"
  // streaming properties
  val defaultBatchInterval: String = "gimel.kafka.throttle.streaming.window.seconds"
  val maxRatePerPartitionKey: String = "gimel.kafka.throttle.streaming.maxRatePerPartition"
  val streamMaxRatePerPartitionKey: String = "gimel.kafka.spark.streaming.kafka.maxRatePerPartition"
  val streamParallelKey: String = "gimel.kafka.throttle.streaming.parallelism.factor"
  val isStreamParallelKey: String = "gimel.kafka.throttle.streaming.isParallel"
  val isBackPressureEnabledKey: String = "gimel.kafka.spark.streaming.backpressure.enabled"
  val streamaWaitTerminationOrTimeoutKey: String = "gimel.kafka.streaming.awaitTerminationOrTimeout"
  val isStreamBatchSwitchEnabledKey: String = "gimel.kafka.stream.batch.switch.enabled"
  val failStreamThresholdKey: String = "gimel.kafka.fail.stream.threshold.message.per.second"
  val streamCutOffThresholdKey: String = "gimel.kafka.batch.to.stream.cutoff.threshold"
  val streamFailureThresholdPerSecondKey: String = "gimel.kafka.fail.stream.threshold.message.per.second"
  val streamFailureWindowFactorKey: String = "gimel.kafka.fail.stream.window.factor"
  val kafkaConsumerReadCheckpointKey: String = "gimel.kafka.reader.checkpoint.save"
  val kafkaConsumerClearCheckpointKey: String = "gimel.kafka.reader.checkpoint.clear"
  val customOffsetRange: String = "gimel.kafka.custom.offset.range"
  // default packages used in Kafka read/write API
  val paypalMetricsReporterValue: String = "com.paypal.kafka.reporters.KafkaClientMetricsReporter"
  val paypalInterceptorClassName: String = "com.paypal.kafka.clients.interceptors.MonitoringConsumerInterceptor"
  val kafkaStorageHandler: String = "org.apache.hadoop.hive.kafka.KafkaStorageHandler"
  val kafkaStringSerializer: String = "org.apache.kafka.common.serialization.StringSerializer"
  val kafkaByteSerializer: String = "org.apache.kafka.common.serialization.ByteArraySerializer"
  val kafkaStringDeSerializer: String = "org.apache.kafka.common.serialization.StringDeserializer"
  val kafkaByteDeSerializer: String = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
}

