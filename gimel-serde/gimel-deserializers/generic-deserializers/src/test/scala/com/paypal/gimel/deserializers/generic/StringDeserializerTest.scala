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

package com.paypal.gimel.deserializers.generic

import org.scalatest._

import com.paypal.gimel.deserializers.generic.conf.GenericDeserializerConfigs
import com.paypal.gimel.serde.common.spark.SharedSparkSession

class StringDeserializerTest extends FunSpec with Matchers with SharedSparkSession {
  val stringDeserializer = new StringDeserializer

  describe("deserialize") {
    it ("should return deserialized dataframe") {
      val dataframeJsonString = mockDataInDataFrameWithJsonString(10)
      val dataFrame = mockDataInDataFrameWithJsonStringBytes(10)
      val deserializedDF = stringDeserializer.deserialize(dataFrame)
      assert(dataframeJsonString.except(deserializedDF).count() == 0)
    }
  }

  describe("deserialize with columnToDeserialize not present in input dataframe") {
    it("it should throw error if " + GenericDeserializerConfigs.columnToDeserializeKey + " is not present in input dataframe") {
      val props = Map(GenericDeserializerConfigs.columnToDeserializeKey -> "val")
      val dataFrame = mockDataInDataFrameWithJsonStringBytes(10)
      val exception = intercept[IllegalArgumentException] {
        stringDeserializer.deserialize(dataFrame, props)
      }
      exception.getMessage.contains(s"Column to Deserialize does not exist in dataframe")
    }
  }
}
