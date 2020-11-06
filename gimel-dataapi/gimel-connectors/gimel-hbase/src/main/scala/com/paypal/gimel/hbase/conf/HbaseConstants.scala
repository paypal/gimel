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

package com.paypal.gimel.hbase.conf

object HbaseConstants {
  // basic variable references
  val DEFAULT_ROW_KEY_COLUMN = "rowKey"
  val DEFAULT_NAMESPACE = "default"

  val SCAN_OPERATION = "scan"
  val GET_OPERATION = "get"
  val PUT_OPERATION = "put"

  val NONE_STRING = "NONE"

  val MAX_SAMPLE_RECORDS_FOR_SCHEMA = "1000"
  val MAX_COLUMNS_FOR_SCHEMA = "100000"
}
