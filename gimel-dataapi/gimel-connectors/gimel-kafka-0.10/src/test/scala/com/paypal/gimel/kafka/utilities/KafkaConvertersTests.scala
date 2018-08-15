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
import com.paypal.gimel.kafka.utilities.KafkaUtilities._

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

  it("should convert a json string of custom partition information to an array of offset ranges") {
    val sampleRange: Array[OffsetRange] = Array(
      OffsetRange("test", 0, 1, 100),
      OffsetRange("test", 1, 1, 100))
    val defaultRange: Array[OffsetRange] = Array(
      OffsetRange("test", 0, 1, 100),
      OffsetRange("test", 2, 1, 100))
    val sampleJson: String =
      """[{"topic":"test","offsetRange":[{"partition":0,"from":1,"to":100},{"partition":1,"from":1,"to":100}]}]"""
    /*
    Happy case for Batch
    The value returned should be a valid conversion of the sampleJson to an Array[OffsetRange]
    */
    val finalOffsetRanges: Array[OffsetRange] = getCustomOffsetRangeForReader("test".split(","), sampleJson, "BATCH")
    finalOffsetRanges shouldEqual(sampleRange)

    val sampleRangeForStream: Array[OffsetRange] = Array(
      OffsetRange("test", 0, 1, 100),
      OffsetRange("test", 1, 1, -1))
    /*
    To offset missing case for Stream
    The value returned should be a valid conversion of the sampleJson to an Array[OffsetRange] with To offset as -1
    */
    val sampleJsonForStream: String =
      """[{"topic":"test","offsetRange":[{"partition":0,"from":1,"to":100},{"partition":1,"from":1}]}]"""
    val finalOffsetRangesForStreamWithoutTo: Array[OffsetRange] = getCustomOffsetRangeForReader("test".split(","), sampleJsonForStream, "STREAM")
    finalOffsetRangesForStreamWithoutTo shouldEqual(sampleRangeForStream)
  }

}

