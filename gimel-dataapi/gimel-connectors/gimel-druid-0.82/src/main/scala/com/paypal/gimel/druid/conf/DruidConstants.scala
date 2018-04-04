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
  * Object Defining Default Values for Configuration
  */
object DruidConstants {
  val TIMESTAMP_FIELD_NAME = "timestamp"
  val TIMESTAMP_FORMAT = "millis"
  val QUERY_GRANULARITY_ONE_MINUTE = "MINUTE"
  val SEGMENT_GRANULARITY_FIFTEEN_MINUTE = "FIFTEEN_MINUTE"
  val WINDOW_PERIOD = "PT10M"
  val PARTITIONS = 1
  val REPLICANTS = 1
  val REALTIME_LOAD = "realtime"
  val BATCH_LOAD = "batch"
  val ARROW = "->"
  val NEW_LINE = "\n"
  val MILLISECONDS = "millis"
  val SECONDS = "seconds"
  val ISO = "iso"
}

