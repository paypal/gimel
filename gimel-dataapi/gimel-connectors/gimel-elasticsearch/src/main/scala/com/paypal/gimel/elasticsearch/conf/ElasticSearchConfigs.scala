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

object ElasticSearchConfigs {
  // elastic search properties
  val esResource: String = "es.resource"
  val esMappingId: String = "es.mapping.id"
  val esMappingNames: String = "es.mapping.names"
  val esIndexAutoCreate: String = "es.index.auto.create"
  // metastore properties
  val esIsPartitioned: String = "gimel.es.index.partition.isEnabled"
  val esDelimiter: String = "gimel.es.index.partition.delimiter"
  // misc properties for read/write
  val esStorageHandler: String = "org.elasticsearch.hadoop.hive.EsStorageHandler"
  val esMapping = "gimel.es.schema.mapping"
  val esDateRichMapping = "es.mapping.date.rich"
  val esPartitionColumn: String = "gimel.es.sql.partition.column"
  val esIsDailyIndex = "gimel.es.sql.partition.dailyIndex"
  val esPartition: String = "gimel.es.index.partition.suffix"
  val esDefaultReadForAllPartitions: String = "gimel.es.index.read.all.partitions.isEnabled"
  val postStatsToESEnabledKey: String = "gimel.es.query.stats.post.enabled"
  val postStatsToLoggerESEnabled: String = "gimel.es.query.stats.post.logger.enabled"
  val esDateRichMappingEnabled: String = "true"
  val esDateRichMappingDisabled: String = "false"
  val esIsPartitionedEnabled: String = "true"
  val esIsPartitionedDisabled: String = "false"
  val esIndexPartitionList = "gimel.es.index.partition.list"
}

