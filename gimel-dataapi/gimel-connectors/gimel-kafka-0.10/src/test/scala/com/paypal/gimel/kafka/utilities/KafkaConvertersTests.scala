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

package com.paypal.gimel.kafka.utilities

import scala.language.implicitConversions

import org.apache.spark.streaming.kafka010.OffsetRange
import org.scalatest._

import com.paypal.gimel.kafka.utilities.ImplicitKafkaConverters._

class KafkaConvertersTests extends FunSpec with Matchers {

  it("should convert array of offset ranges to a single parsable string") {
    val sampleRange: Array[OffsetRange] = Array(
      OffsetRange("test", 0, 1, 100),
      OffsetRange("test", 1, 1, 100))
    val stringified = sampleRange.toStringOfKafkaOffsetRanges
    stringified shouldBe "test,0,1,100|test,1,1,100"
  }

  it("should converr offset Range to a single parsable checkPoint String") {
    val sampleRange = OffsetRange("test", 0, 1, 100)
    val stringiFied = sampleRange.toStringOfKafkaOffsetRange
    stringiFied shouldBe "test,0,1,100"
  }

  it("should convert a single parsable CheckPoint string to a valid offset Range") {
    val sampleString = "test,0,1,100"
    val offsetRange = CheckPointString(sampleString).toKafkaOffsetRange
    offsetRange shouldBe OffsetRange("test", 0, 1, 100)
  }

  it("should convert composite `CheckPoint (Array[String])` to a valid Array(Offset Range)") {
    val expectedOffsetRanges = Array(OffsetRange("test", 0, 1, 100), OffsetRange("test", 1, 1, 101))
    val sampleString: Array[String] = "test,0,1,100|test,1,1,101".split('|')
    val offsetRanges: Array[OffsetRange] = sampleString.map(CheckPointString).toKafkaOffsetRanges
    offsetRanges shouldEqual expectedOffsetRanges
  }

}

