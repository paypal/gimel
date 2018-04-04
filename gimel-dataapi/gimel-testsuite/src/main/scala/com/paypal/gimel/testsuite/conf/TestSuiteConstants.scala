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

package com.paypal.gimel.testsuite.conf

object TestSuiteConstants {
  val statsApplicationID: String = "app_id"
  val statsApplicationName: String = "app_name"
  val statsApplicationTag: String = "app_tag"
  val statsLogType: String = "logType"
  val statsSourceDataSetName: String = "source_data_set_name"
  val statsDatasetSystemType: String = "dataset_system_type"
  val statsSourceRecordsCount: String = "source_records_count"
  val statsSourceOffsetDetails: String = "source_offset_details"
  val statsSourceLaggingRecordsCount: String = "source_lagging_records_count"
  val statsSourceLaggingOffsetDetails: String = "source_lagging_offset_details"
  val statsLogTime: String = "logtime"
  val storagesToBenchmarkKey: String = "storages"
  val topicKey: String = "cdh_topic"
  val minRowsPerPartitionKey: String = "minRowsPerParallel"
  val maxRecordsPerPartitionKey: String = "maxRecordsPerPartition"
  val fetchRowsKey: String = "fetchRowsOnFirstRun"
}

