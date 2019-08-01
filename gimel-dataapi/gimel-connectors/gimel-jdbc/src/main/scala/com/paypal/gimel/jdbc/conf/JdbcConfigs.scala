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

package com.paypal.gimel.jdbc.conf

// keys related to teradata
object JdbcConfigs {
  // metastore properties
  val jdbcDriverClassKey: String = "gimel.jdbc.driver.class"
  val jdbcInputTableNameKey: String = "gimel.jdbc.input.table.name"
  val jdbcUrl: String = "gimel.jdbc.url"

  // misc properties for read/write
  val jdbcStorageHandler: String = "org.apache.hadoop.hive.jdbc.storagehandler.JdbcStorageHandler"
  val teradataReadType: String = "gimel.jdbc.read.type"
  val teradataWriteType: String = "gimel.jdbc.write.type"
  val jdbcOutputTableNameKey: String = "gimel.jdbc.output.table.name"
  val jdbcP: String = "gimel.jdbc.p.file"
  val jdbcPassword: String = "gimel.jdbc.password"
  val jdbcPushDownEnabled: String = "gimel.jdbc.enableQueryPushdown"
  val jdbcInsertStrategy: String = "gimel.jdbc.insertStrategy"
  val jdbcInputDataPartitionCount: String = "gimel.jdbc.inputDataPartition.count"
  val jdbcPasswordStrategy: String = "gimel.jdbc.p.strategy"
  val jdbcUpdateSetColumns: String = "gimel.jdbc.update.setColumns"
  val jdbcUpdateWhereColumns: String = "gimel.jdbc.update.whereColumns"

}

