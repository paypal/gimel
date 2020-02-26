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

package com.paypal.gimel.serde.common.schema

import scala.collection.mutable

import org.scalatest._

import com.paypal.gimel.logger.Logger
import com.paypal.gimel.serde.common.kafka.EmbeddedSingleNodeKafkaCluster
import com.paypal.gimel.serde.common.spark.SharedSparkSession

class ConfluentSchemaRegistryTest extends FunSpec with Matchers with SharedSparkSession {

  val avroSchema = s"""{"namespace": "namespace",
          "type": "record",
          "name": "test",
          "fields": [
              {\"name\": \"null\", \"type\": \"null\"},
              {\"name\": \"boolean\", \"type\": \"boolean\"},
              {\"name\": \"int\", \"type\": \"int\"},
              {\"name\": \"long\", \"type\": \"long\"},
              {\"name\": \"float\", \"type\": \"float\"},
              {\"name\": \"double\", \"type\": \"double\"},
              {\"name\": \"bytes\", \"type\": \"bytes\"},
              {\"name\": \"string\", \"type\": \"string\", \"aliases\": [\"string_alias\"]},
              {\"name\": \"null_default\", \"type\": \"null\", \"default\": null},
              {\"name\": \"boolean_default\", \"type\": \"boolean\", \"default\": false},
              {\"name\": \"int_default\", \"type\": \"int\", \"default\": 24},
              {\"name\": \"long_default\", \"type\": \"long\", \"default\": 4000000000},
              {\"name\": \"float_default\", \"type\": \"float\", \"default\": 12.3},
              {\"name\": \"double_default\", \"type\": \"double\", \"default\": 23.2},
              {\"name\": \"bytes_default\", \"type\": \"bytes\", \"default\": \"bytes\"},
              {\"name\": \"string_default\", \"type\": \"string\", \"default\": \"default string\"}
         ]
         }""".stripMargin

  val kafkaCluster = new EmbeddedSingleNodeKafkaCluster()
  val logger = Logger()
  logger.setLogLevel("INFO")
  logger.consolePrintEnabled = true

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaCluster.start()
    kafkaCluster.schemaRegistry.restClient.registerSchema(avroSchema, "test_schema")
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    kafkaCluster.stop()
  }

  describe("getAllSubjectAndSchema") {
    it ("get all schemas for all subjects from schema registry") {
      val allSchemasSubjects: Map[String, (String, mutable.Map[Int, String])] = SchemaRegistryLookUp.getAllSubjectAndSchema(kafkaCluster.schemaRegistryUrl())
      allSchemasSubjects.keys.sameElements(Array("test_schema"))
      allSchemasSubjects.get("test_schema").get._1.shouldBe(s"""{"type":"record","name":"test","namespace":"namespace","fields":[{"name":"null","type":"null"},{"name":"boolean","type":"boolean"},{"name":"int","type":"int"},{"name":"long","type":"long"},{"name":"float","type":"float"},{"name":"double","type":"double"},{"name":"bytes","type":"bytes"},{"name":"string","type":"string","aliases":["string_alias"]},{"name":"null_default","type":"null","default":null},{"name":"boolean_default","type":"boolean","default":false},{"name":"int_default","type":"int","default":24},{"name":"long_default","type":"long","default":4000000000},{"name":"float_default","type":"float","default":12.3},{"name":"double_default","type":"double","default":23.2},{"name":"bytes_default","type":"bytes","default":"bytes"},{"name":"string_default","type":"string","default":"default string"}]}""")
      allSchemasSubjects.get("test_schema").get._2.getOrElse(1, "").shouldBe(s"""{"type":"record","name":"test","namespace":"namespace","fields":[{"name":"null","type":"null"},{"name":"boolean","type":"boolean"},{"name":"int","type":"int"},{"name":"long","type":"long"},{"name":"float","type":"float"},{"name":"double","type":"double"},{"name":"bytes","type":"bytes"},{"name":"string","type":"string","aliases":["string_alias"]},{"name":"null_default","type":"null","default":null},{"name":"boolean_default","type":"boolean","default":false},{"name":"int_default","type":"int","default":24},{"name":"long_default","type":"long","default":4000000000},{"name":"float_default","type":"float","default":12.3},{"name":"double_default","type":"double","default":23.2},{"name":"bytes_default","type":"bytes","default":"bytes"},{"name":"string_default","type":"string","default":"default string"}]}""")
    }
  }

  describe("getAllSchemasForSubject") {
    it ("should get latest schema and map of version to schemas for a subject from schema registry") {
      val allSchemaSubjects: (String, mutable.Map[Int, String]) = SchemaRegistryLookUp.getAllSchemasForSubject("test_schema", kafkaCluster.schemaRegistryUrl())
      allSchemaSubjects._1.shouldBe(s"""{"type":"record","name":"test","namespace":"namespace","fields":[{"name":"null","type":"null"},{"name":"boolean","type":"boolean"},{"name":"int","type":"int"},{"name":"long","type":"long"},{"name":"float","type":"float"},{"name":"double","type":"double"},{"name":"bytes","type":"bytes"},{"name":"string","type":"string","aliases":["string_alias"]},{"name":"null_default","type":"null","default":null},{"name":"boolean_default","type":"boolean","default":false},{"name":"int_default","type":"int","default":24},{"name":"long_default","type":"long","default":4000000000},{"name":"float_default","type":"float","default":12.3},{"name":"double_default","type":"double","default":23.2},{"name":"bytes_default","type":"bytes","default":"bytes"},{"name":"string_default","type":"string","default":"default string"}]}""")
      allSchemaSubjects._2.getOrElse(1, "").shouldBe(s"""{"type":"record","name":"test","namespace":"namespace","fields":[{"name":"null","type":"null"},{"name":"boolean","type":"boolean"},{"name":"int","type":"int"},{"name":"long","type":"long"},{"name":"float","type":"float"},{"name":"double","type":"double"},{"name":"bytes","type":"bytes"},{"name":"string","type":"string","aliases":["string_alias"]},{"name":"null_default","type":"null","default":null},{"name":"boolean_default","type":"boolean","default":false},{"name":"int_default","type":"int","default":24},{"name":"long_default","type":"long","default":4000000000},{"name":"float_default","type":"float","default":12.3},{"name":"double_default","type":"double","default":23.2},{"name":"bytes_default","type":"bytes","default":"bytes"},{"name":"string_default","type":"string","default":"default string"}]}""")
    }
  }
}
