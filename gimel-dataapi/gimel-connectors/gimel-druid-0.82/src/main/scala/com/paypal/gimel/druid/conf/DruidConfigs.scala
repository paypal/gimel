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

package com.paypal.gimel.druid.conf

/**
  * Object Defining List of available Configuration Keys
  */
object DruidConfigs {
  val ZOOKEEPER = "gimel.druid.zookeeper.hosts"
  val INDEX_SERVICE = "gimel.druid.cluster.index.service"
  val DISCOVERY_PATH = "gimel.druid.cluster.discovery.path"
  val DATASOURCE = "gimel.druid.datasource.name"
  val FIELDS = "gimel.druid.datasource.fields"
  val DIMENSIONS = "gimel.druid.datasource.dimensions"
  val METRICS = "gimel.druid.datasource.metrics"
  val TIMESTAMP = "gimel.druid.timestamp.fieldname"
  val TIMESTAMP_FORMAT = "gimel.druid.timestamp.format"
  val QUERY_GRANULARITY = "gimel.druid.query.granularity"
  val SEGMENT_GRANULARITY = "gimel.druid.segment.granularity"
  val WINDOW_PERIOD = "gimel.druid.stream.window.period"
  val PARTITIONS = "gimel.druid.datasource.partitions"
  val REPLICANTS = "gimel.druid.datasource.replicas"
  val LOAD_TYPE = "gimel.druid.ingestion.type"
}

