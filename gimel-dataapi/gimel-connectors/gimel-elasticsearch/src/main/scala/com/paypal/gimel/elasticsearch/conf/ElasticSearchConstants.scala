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

package com.paypal.gimel.elasticsearch.conf

object ElasticSearchConstants {
  val slashSeparator: String = "/"
  val colon: String = ":"
  val aliases: String = "_aliases"
  val hiveTableProps: String = "hiveTableProps"
  val esRowHashId: String = "hashId"
  val esReadFlag: Int = 1
  val esWriteFlag: Int = 2
  val defaultDelimiter: String = "_"
  val defaultReadAllFlag: String = "false"
  val defaultPartitionsIsEnabled: String = "false"
  val dataFrameES2Mapping: Map[String, String] = Map(
    "null" -> "null", "ByteType" -> "byte",
    "ShortType" -> "short", "IntegerType" -> "integer", "LongType" -> "long", "FloatType" -> "float",
    "DoubleType" -> "double", "StringType" -> "string", "BinaryType" -> "string", "BooleanType" -> "boolean",
    "TimestampType" -> "long", "ArrayType" -> "array", "MapType" -> "object", "StructType" -> "object",
    "DateType" -> "date")

  val dataFrameES6Mapping: Map[String, String] = Map(
    "null" -> "null", "ByteType" -> "byte",
    "ShortType" -> "short", "IntegerType" -> "integer", "LongType" -> "long", "FloatType" -> "float",
    "DoubleType" -> "double", "StringType" -> "text", "BinaryType" -> "text", "BooleanType" -> "boolean",
    "TimestampType" -> "long", "ArrayType" -> "array", "MapType" -> "object", "StructType" -> "object",
    "DateType" -> "date")

  val whiteListIndicesDefault =
    """[{"prefix": "hdfsaudit-cluster1","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "sparkjoblogs-cluster2","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "jobconf-cluster1","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "hdfsaudit-cluster2","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "hdfsimage-cluster1","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix":"sparkjobmetrics-cluster1","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "jobhistory-cluster1","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "sparktaskmetrics-cluster1","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "jobconf-cluster2","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "sparkjoblogs-cluster1","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "hdfsimage-cluster2","delimiter": "-","suffix": "YYYYMMDD"},
      |{"prefix": "jobhistory-cluster2","delimiter": "-","suffix": "YYYYMMDD"}]
      |""".stripMargin
  // es smoke tests
  val esPollingSmokeTestIndex = "pc_smoke_test_index"
  val esPollingLogstashTestIndex = "logstash-2015.05.18"
  val esPollingSmokeTestStreamingIndex = "pc_smoketest_kafka_streaming_es"
  val esPollingKibanaIndex = ".kibana"
}
