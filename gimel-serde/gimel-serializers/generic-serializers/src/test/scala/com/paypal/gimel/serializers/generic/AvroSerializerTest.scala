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

package com.paypal.gimel.serializers.generic

import org.scalatest._

import com.paypal.gimel.logger.Logger
import com.paypal.gimel.serde.common.avro.AvroUtils
import com.paypal.gimel.serde.common.kafka.EmbeddedSingleNodeKafkaCluster
import com.paypal.gimel.serde.common.spark.SharedSparkSession
import com.paypal.gimel.serializers.generic.conf.{GenericSerializerConfigs, GenericSerializerConstants}

class AvroSerializerTest extends FunSpec with Matchers with SharedSparkSession {
  val empAvroSchema =
    s"""{"namespace": "namespace",
          "type": "record",
          "name": "test_emp",
          "fields": [
              {\"name\": \"address\", \"type\": \"string\"},
              {\"name\": \"age\", \"type\": \"string\"},
              {\"name\": \"company\", \"type\": \"string\"},
              {\"name\": \"designation\", \"type\": \"string\"},
              {\"name\": \"id\", \"type\": \"string\"},
              {\"name\": \"name\", \"type\": \"string\"},
              {\"name\": \"salary\", \"type\": \"string\"}
         ]}""".stripMargin

  val avroSerializer = new AvroSerializer

  val kafkaCluster = new EmbeddedSingleNodeKafkaCluster()
  val logger = Logger()
  logger.setLogLevel("INFO")
  logger.consolePrintEnabled = true

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaCluster.start()
    kafkaCluster.schemaRegistry.restClient.registerSchema(empAvroSchema, "test_emp")
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    kafkaCluster.stop()
  }

  describe ("serialize with schema passed inline") {
    it("it should return a serialized dataframe") {
      val props = Map(GenericSerializerConfigs.avroSchemaStringKey -> empAvroSchema)
      val dataFrame = mockDataInDataFrame(10)
      val serializedDF = avroSerializer.serialize(dataFrame, props)
      val deserializedDF = AvroUtils.getDeserializedDataFrame(serializedDF, "value", empAvroSchema)
      assert(deserializedDF.columns.sorted.sameElements(dataFrame.columns.sorted))
      assert(deserializedDF.except(dataFrame).count() == 0)
    }

    it("it should throw error if " + GenericSerializerConfigs.avroSchemaStringKey + " is empty or not found") {
      val dataFrame = mockDataInDataFrame(10)
      val exception = intercept[IllegalArgumentException] {
        avroSerializer.serialize(dataFrame)
      }
      exception.getMessage.contains(s"You need to provide avro schema string with schema source ${GenericSerializerConstants.avroSchemaInline}")
    }
  }

  describe("serialize with avro schema stored in schema registry") {
    it ("it should return a serialized dataframe") {
      val props = Map(GenericSerializerConfigs.avroSchemaSourceUrlKey -> kafkaCluster.schemaRegistryUrl(),
        GenericSerializerConfigs.avroSchemaSourceKey -> GenericSerializerConstants.avroSchemaCSR,
        GenericSerializerConfigs.avroSchemaSubjectKey -> "test_emp")
      logger.info(props)
      val dataFrame = mockDataInDataFrame(10)
      val serializedDF = avroSerializer.serialize(dataFrame, props)
      val deserializedDF = AvroUtils.getDeserializedDataFrame(serializedDF, "value", empAvroSchema)
      assert(deserializedDF.columns.sorted.sameElements(dataFrame.columns.sorted))
      assert(deserializedDF.except(dataFrame).count() == 0)
    }

    it("it should throw error if " + GenericSerializerConfigs.avroSchemaSubjectKey + " is empty or not found") {
      val props = Map(GenericSerializerConfigs.avroSchemaSourceUrlKey -> kafkaCluster.schemaRegistryUrl(),
        GenericSerializerConfigs.avroSchemaSourceKey -> GenericSerializerConstants.avroSchemaCSR)
      val dataFrame = mockDataInDataFrame(10)
      val exception = intercept[IllegalArgumentException] {
        avroSerializer.serialize(dataFrame)
      }
      exception.getMessage.contains(s"You need to provide schema subject with schema source ${GenericSerializerConstants.avroSchemaCSR}")
    }
  }

  describe("serialize with avro schema source unknown") {
    it("it should throw error if " + GenericSerializerConfigs.avroSchemaSourceKey + " is empty or unknown") {
      val props = Map(GenericSerializerConfigs.avroSchemaSourceUrlKey -> kafkaCluster.schemaRegistryUrl(),
        GenericSerializerConfigs.avroSchemaSourceKey -> "TEST")
      val dataFrame = mockDataInDataFrame(10)
      val exception = intercept[IllegalArgumentException] {
        avroSerializer.serialize(dataFrame)
      }
      exception.getMessage.contains(s"Unknown value of Schema Source")
    }
  }

}
