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

package com.paypal.gimel.common.utilities

import org.scalatest._

/**
  * The test suit for DataSetUtilTest.
  */
class DataSetUtilsSpec extends FunSpec with Matchers {

  it("should throw exception for null input.") {
    val exception = intercept[NullPointerException] {
      DataSetUtils.getProps(null)
    }
    exception.getMessage shouldBe null
  }

  it("should throw exception for malformed str input.") {
    val exception = intercept[Exception] {
      DataSetUtils.getProps("key1=key2=val1;key3=val3")
    }
    exception.getMessage.contains("Error") shouldBe true
    exception.getMessage.contains("key1=key2=val1;key3=val3") shouldBe true
  }

  it("should throw exception for str input key-values are not separated by semicolon.") {
    val exception = intercept[Exception] {
      DataSetUtils.getProps("key1=key2=val1,key3=val3")
    }
    exception.getMessage.contains("Error") shouldBe true
    exception.getMessage.contains("key1=key2=val1,key3=val3") shouldBe true
  }

  it("should return empty Map for the empty string input.") {
    DataSetUtils.getProps("") shouldBe (Map.empty)
  }

  it("should return empty for empty Map inputs.") {
    val expectedResult = Map.empty
    DataSetUtils.getProps(Map.empty) shouldBe (expectedResult)
  }

  it("should return the right value for valid str input.") {
    val expectedResult = Map("key1" -> "val1", "key2" -> "val2")
    DataSetUtils.getProps("key1=val1:key2=val2") shouldEqual expectedResult
  }

  it("should return the right value for valid map input.") {
    val expectedResult = Map("key1" -> "val1", "key2" -> "val2")
    // same map, but in a different order
    val input: Any = Map[String, Any]("key2" -> "val2", "key1" -> "val1")

    DataSetUtils.getProps(input) shouldEqual expectedResult
  }
}
