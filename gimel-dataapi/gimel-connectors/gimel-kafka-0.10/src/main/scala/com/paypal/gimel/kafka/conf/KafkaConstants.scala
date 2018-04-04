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

object KafkaConstants {
  // basic variable references
  val gimelKafkaAvroSchemaCDH = "CDH"
  val gimeKafkaAvroSchemaCSR = "CSR"
  val gimelKafkaAvroSchemaInline = "INLINE"
  val gimelAuditRunTypeBatch = "BATCH"
  val gimelAuditRunTypeStream = "STREAM"
  val gimelAuditRunTypeIntelligent = "INTELLIGENT"
  val cluster = "cluster"
  // polling properties
  val unknownContainerName = "unknown"
  val kafkaAllTopics = "All"
  val targetDb = "pcatalog"
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
}

