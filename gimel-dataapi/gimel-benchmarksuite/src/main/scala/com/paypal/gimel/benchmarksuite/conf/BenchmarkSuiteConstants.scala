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

package com.paypal.gimel.benchmarksuite.conf

// Keys related to benchmarking suite
object BenchmarkSuiteConstants {
  val storageHandlerKey: String = "storage_handler"
  val storagesToBenchmarkKey: String = "storages"
  val storagesToBenchmarkValue: String = "hive,elasticsearch,kafka_cdh,kafka_string,kafka_avro"
  val clusterNameKey: String = "clusterName"
  val minRowsPerPartitionKey: String = "minRowsPerParallel"
  val maxRecordsPerPartitionKey: String = "maxRecordsPerPartition"
  val fetchRowsKey: String = "fetchRowsOnFirstRun"
  val parallelsPerPartition: String = "parallelsPerPartition"
  val topicKey: String = "cdh_topic"
}

