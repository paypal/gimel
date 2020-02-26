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

package com.paypal.gimel.deserializers.generic.conf

object GenericDeserializerConfigs {
  // Options for Avro Deserializer
  val avroSchemaSourceKey: String = "gimel.deserializer.avro.schema.source"
  val avroSchemaSourceUrlKey: String = "gimel.deserializer.avro.schema.url"
  val avroSchemaSubjectKey: String = "gimel.deserializer.avro.schema.subject"
  val avroSchemaStringKey: String = "gimel.deserializer.avro.schema.string"
  val columnToDeserializeKey: String = "gimel.deserializer.column.name"

  val fieldsBindToJson: String = "gimel.fields.bind.to.json"

  // Options for Gimel Kafka1 API Backward compatibility
  val avroSchemaSourceKafka1: String = "gimel.kafka.avro.schema.source"
  val avroSchemaSourceUrlKafka1: String = s"${avroSchemaSourceKafka1}.url"
  val avroSchemaSourceKeyKafka1: String = s"${avroSchemaSourceKafka1}.key"
  val avroSchemaStringKeyKafka1: String = "gimel.kafka.avro.schema.string"
}
