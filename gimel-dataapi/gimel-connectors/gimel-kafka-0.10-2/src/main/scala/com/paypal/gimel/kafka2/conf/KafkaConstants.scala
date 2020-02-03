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

object KafkaConstants {
  // basic variable references
  val gimelAuditRunTypeBatch = "BATCH"
  val gimelAuditRunTypeStream = "STREAM"
  val gimelAuditRunTypeIntelligent = "INTELLIGENT"
  val cluster = "cluster"
  // polling properties
  val kafkaSchemaRegistry = "http://localhost:8081"
  val kafkaAllTopics = "All"
  val targetDb = "pcatalog"
  val confluentSchemaUrl = "confluent_schema_url"
  val generateDdlKey = "generate_ddl_for"
  val targetDbkey = "target_db"
  val avroToHiveTypes = Map(
    "null" -> "void",
    "boolean" -> "boolean",
    "int" -> "int",
    "long" -> "bigint",
    "float" -> "float",
    "double" -> "double",
    "bytes" -> "binary",
    "string" -> "string",
    "record" -> "struct",
    "map" -> "map",
    "list" -> "array",
    "union" -> "union",
    "enum" -> "string",
    "fixed" -> "binary")
  // STRUCTURED STREAMING SPARK CONSTANTS
  val KAFKA_FORMAT: String = "kafka"
  val KAFKA_BOOTSTRAP_SERVERS: String = "kafka.bootstrap.servers"
  val KAFKA_SUBSCRIBE: String = "subscribe"
  val KAFKA_TOPIC: String = "topic"
  val KAFKA_START_OFFSETS: String = "startingOffsets"
  val KAFKA_END_OFFSETS: String = "endingOffsets"
  val STREAM_FAIL_ON_DATA_LOSS: String = "failOnDataLoss"
  val KAFKA_MAX_OFFSETS_PER_TRIGGER: String = "maxOffsetsPerTrigger"
  val KAFKA_POLL_TIMEOUT: String = "kafkaConsumer.pollTimeoutMs"
  val KAFKA_FETCH_RETRIES: String = "fetchOffset.numRetries"
  val KAFKA_RETRY_INTERVAL: String = "fetchOffset.retryIntervalMs"
  val earliestOffset: String = "earliest"
  val allKafkaSourceFields: String = "all"
}

