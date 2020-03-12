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

package com.paypal.gimel.common.utilities.gimelserde

import org.scalatest._

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.gimelserde.{GimelSerDe, GimelSerdeUtils}
import com.paypal.gimel.common.utilities.spark.SharedSparkSession

class GimelSerdeUtilsTest extends FunSpec with Matchers with SharedSparkSession {
  describe ("getSerdeClass") {
    it ("should return GimelSerde case class based on the kafka.message.value.type and value.serializer properties") {
      // String serde
      val props1 : Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "string",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_STRING_SERIALIZER)
      val dataSetProperties1 = DataSetProperties("KAFKA", null, null, props1)
      GimelSerdeUtils.getSerdeClass(spark, dataSetProperties1, false)
        .shouldBe(GimelSerDe(GimelConstants.GIMEL_STRING_DESERIALIZER_CLASS_DEFAULT,
          GimelConstants.GIMEL_STRING_SERIALIZER_CLASS_DEFAULT))

      // Json serde
      val props2 : Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "json",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_STRING_SERIALIZER)
      val dataSetProperties2 = DataSetProperties("KAFKA", null, null, props2)
      GimelSerdeUtils.getSerdeClass(spark, dataSetProperties2, false)
        .shouldBe(GimelSerDe(GimelConstants.GIMEL_JSON_DYNAMIC_DESERIALIZER_CLASS_DEFAULT,
          GimelConstants.GIMEL_JSON_SERIALIZER_CLASS_DEFAULT))

      // Binary serde
      val props3 : Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "binary",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_BYTE_SERIALIZER)
      val dataSetProperties3 = DataSetProperties("KAFKA", null, null, props3)
      GimelSerdeUtils.getSerdeClass(spark, dataSetProperties3, false)
        .shouldBe(GimelSerDe(GimelConstants.GIMEL_BINARY_DESERIALIZER_CLASS_DEFAULT, ""))

      // Avro serde
      val props4 : Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_BYTE_SERIALIZER)
      val dataSetProperties4 = DataSetProperties("KAFKA", null, null, props4)
      GimelSerdeUtils.getSerdeClass(spark, dataSetProperties4, false)
        .shouldBe(GimelSerDe(GimelConstants.GIMEL_AVRO_DESERIALIZER_CLASS_DEFAULT,
          GimelConstants.GIMEL_AVRO_SERIALIZER_CLASS_DEFAULT))

      // Json streaming serde
      val props5 : Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "json",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_STRING_SERIALIZER)
      val dataSetProperties5 = DataSetProperties("KAFKA", null, null, props5)
      GimelSerdeUtils.getSerdeClass(spark, dataSetProperties5, true)
        .shouldBe(GimelSerDe(GimelConstants.GIMEL_JSON_STATIC_DESERIALIZER_CLASS_DEFAULT, ""))

      // Invalid combination
      val props6 : Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "json",
        GimelConstants.SERIALIZER_VALUE -> "test")
      val dataSetProperties6 = DataSetProperties("KAFKA", null, null, props6)
      GimelSerdeUtils.getSerdeClass(spark, dataSetProperties6, true)
        .shouldBe(GimelSerDe("", ""))
    }
  }

  describe ("setGimelDeserializer") {
    it ("should set the gimel deserializer class in the options passed") {
      // Should set the gimel.deserializer.class in input options
      val props1 : Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "string",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_STRING_SERIALIZER)
      val dataSetProperties1 = DataSetProperties("KAFKA", null, null, props1)
      val newOptions1 = GimelSerdeUtils.setGimelDeserializer(spark, dataSetProperties1, props1)
      newOptions1.get(GimelConstants.GIMEL_DESERIALIZER_CLASS).get.toString
        .shouldBe(GimelConstants.GIMEL_STRING_DESERIALIZER_CLASS_DEFAULT)

      // Should set the gimel.deserializer.class in input options
      val props2 : Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "json",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_STRING_SERIALIZER)
      val dataSetProperties2 = DataSetProperties("KAFKA", null, null, props2)
      val newOptions = GimelSerdeUtils.setGimelDeserializer(spark, dataSetProperties2, props2)
      newOptions.get(GimelConstants.GIMEL_DESERIALIZER_CLASS).get.toString
        .shouldBe(GimelConstants.GIMEL_JSON_DYNAMIC_DESERIALIZER_CLASS_DEFAULT)

      // Should set the gimel.deserializer.class in input options in stream
      val props3 : Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "json",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_STRING_SERIALIZER)
      val dataSetProperties3 = DataSetProperties("KAFKA", null, null, props3)
      val newOptions3 = GimelSerdeUtils.setGimelDeserializer(spark, dataSetProperties3, props3, true)
      newOptions3.get(GimelConstants.GIMEL_DESERIALIZER_CLASS).get.toString
        .shouldBe(GimelConstants.GIMEL_JSON_STATIC_DESERIALIZER_CLASS_DEFAULT)
    }
  }

  describe ("setGimelSerializer") {
    it("should set the gimel serializer class in the options passed") {
      // Should set the gimel.serializer.class in input options
      val props1: Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "string",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_STRING_SERIALIZER)
      val dataSetProperties1 = DataSetProperties("KAFKA", null, null, props1)
      val newOptions1 = GimelSerdeUtils.setGimelSerializer(spark, dataSetProperties1, props1)
      newOptions1.get(GimelConstants.GIMEL_SERIALIZER_CLASS).get.toString
        .shouldBe(GimelConstants.GIMEL_STRING_SERIALIZER_CLASS_DEFAULT)

      // Should set the gimel.serializer.class in input options
      val props2: Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "json",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_STRING_SERIALIZER)
      val dataSetProperties2 = DataSetProperties("KAFKA", null, null, props2)
      val newOptions = GimelSerdeUtils.setGimelSerializer(spark, dataSetProperties2, props2)
      newOptions.get(GimelConstants.GIMEL_SERIALIZER_CLASS).get.toString
        .shouldBe(GimelConstants.GIMEL_JSON_SERIALIZER_CLASS_DEFAULT)

      // Should not set the gimel.serializer.class in input options in stream
      val props3: Map[String, String] = Map(GimelConstants.KAFKA_MESSAGE_VALUE_TYPE -> "json",
        GimelConstants.SERIALIZER_VALUE -> GimelConstants.KAFKA_STRING_SERIALIZER)
      val dataSetProperties3 = DataSetProperties("KAFKA", null, null, props3)
      val newOptions3 = GimelSerdeUtils.setGimelSerializer(spark, dataSetProperties3, props3, true)
      newOptions3.get(GimelConstants.GIMEL_SERIALIZER_CLASS).shouldBe(None)
    }
  }
}
