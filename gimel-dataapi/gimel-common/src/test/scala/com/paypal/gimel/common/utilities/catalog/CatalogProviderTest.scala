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

package com.paypal.gimel.common.utilities.catalog

import org.scalatest._

import com.paypal.gimel.common.catalog.CatalogProvider

class CatalogProviderTest extends FunSpec with Matchers {

  describe("getDataSetProperties") {
    it ("should return the DataSetProperties object for a given dataset") {
      val dataSetProperties_json = """{
          "datasetType": "KAFKA",
          "fields": [],
          "partitionFields": [],
          "props": {
              "gimel.storage.type":"KAFKA",
              "bootstrap.servers":"kafka:9092",
              "gimel.kafka.whitelist.topics":"gimel.test.json",
              "zookeeper.connection.timeout.ms":"10000",
              "gimel.kafka.checkpoint.zookeeper.host":"zookeeper:2181",
              "gimel.kafka.checkpoint.zookeeper.path":"/pcatalog/kafka_consumer/checkpoint",
              "auto.offset.reset":"earliest",
              "datasetName":"udc.kafka_test_json",
              "gimel.deserializer.class":"com.paypal.gimel.deserializers.generic.JsonDynamicDeserializer",
              "gimel.serializer.class":"com.paypal.gimel.serializers.generic.JsonSerializer"
          }
      }"""
      val options = Map("udc.kafka_test_json.dataSetProperties" -> dataSetProperties_json,
        "gimel.kafka.throttle.batch.fetchRowsOnFirstRun" -> 1000,
        "gimel.kafka.throttle.batch.parallelsPerPartition" -> 250,
        "gimel.catalog.provider"->"USER")
      CatalogProvider.getDataSetProperties("udc.kafka_test_json", options).getClass.getTypeName
        .shouldBe("com.paypal.gimel.common.catalog.DataSetProperties")
    }

    it ("should return the DataSetProperties for a given dataset from cache table if it exists") {
      val dataSetProperties_json = """{
          "datasetType": "KAFKA",
          "fields": [],
          "partitionFields": [],
          "props": {
              "gimel.storage.type":"KAFKA",
              "bootstrap.servers":"kafka:9092",
              "gimel.kafka.whitelist.topics":"gimel.test.json1",
              "zookeeper.connection.timeout.ms":"10000",
              "gimel.kafka.checkpoint.zookeeper.host":"zookeeper:2181",
              "gimel.kafka.checkpoint.zookeeper.path":"/pcatalog/kafka_consumer/checkpoint",
              "auto.offset.reset":"earliest",
              "datasetName":"udc.kafka_test_json1",
              "gimel.deserializer.class":"com.paypal.gimel.deserializers.generic.JsonDynamicDeserializer",
              "gimel.serializer.class":"com.paypal.gimel.serializers.generic.JsonSerializer"
          }
      }"""
      val options = Map("udc.kafka_test_json1.dataSetProperties" -> dataSetProperties_json,
        "gimel.kafka.throttle.batch.fetchRowsOnFirstRun" -> 1000,
        "gimel.kafka.throttle.batch.parallelsPerPartition" -> 250,
        "gimel.catalog.provider" -> "USER")
      CatalogProvider.getDataSetProperties("udc.kafka_test_json1", options).getClass.getTypeName
        .shouldBe("com.paypal.gimel.common.catalog.DataSetProperties")

      // Cache table should contain the dataset names for which we have already made a call
      CatalogProvider.cachedDataSetPropsMap.keys.sameElements(Set("udc.kafka_test_json", "udc.kafka_test_json1"))
      // Second call to getDataSetProperties should return object from cache
      CatalogProvider.getDataSetProperties("udc.kafka_test_json1", options).getClass.getTypeName
        .shouldBe("com.paypal.gimel.common.catalog.DataSetProperties")
    }
  }
}
