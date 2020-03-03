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

import com.paypal.gimel.common.conf.GimelConstants

// keys related to jdbc api
object JdbcConfigs {
  // metastore properties
  import com.paypal.gimel.common.conf.GimelConstants.GIMEL_JDBC_OPTION_KEYWORD

  val jdbcDriverClassKey: String = s"${GIMEL_JDBC_OPTION_KEYWORD}driver.class"
  val jdbcInputTableNameKey: String =
    s"${GIMEL_JDBC_OPTION_KEYWORD}input.table.name"
  val jdbcUserName: String = s"${GIMEL_JDBC_OPTION_KEYWORD}${GimelConstants.USER_NAME}"
  val jdbcUrl: String = s"${GIMEL_JDBC_OPTION_KEYWORD}url"
  val jdbcDbTable: String = s"${GIMEL_JDBC_OPTION_KEYWORD}dbtable"
  val jdbcTempTable: String = s"${GIMEL_JDBC_OPTION_KEYWORD}temp.table"
  val jdbcTempDatabase: String = s"${GIMEL_JDBC_OPTION_KEYWORD}temp.db"
  // misc properties for read/write
  val jdbcStorageHandler: String =
    "org.apache.hadoop.hive.jdbc.storagehandler.JdbcStorageHandler"
  val teradataReadType: String = s"${GIMEL_JDBC_OPTION_KEYWORD}read.type"
  val teradataWriteType: String = s"${GIMEL_JDBC_OPTION_KEYWORD}write.type"
  val jdbcOutputTableNameKey: String =
    s"${GIMEL_JDBC_OPTION_KEYWORD}output.table.name"
  val jdbcP: String = s"${GIMEL_JDBC_OPTION_KEYWORD}p.file"
  val jdbcPassword: String = "gimel.jdbc.password"
  val jdbcPushDownEnabled: String =
    s"${GIMEL_JDBC_OPTION_KEYWORD}enableQueryPushdown"
  val jdbcInsertStrategy: String = s"${GIMEL_JDBC_OPTION_KEYWORD}insertStrategy"
  val jdbcInputDataPartitionCount: String =
    s"${GIMEL_JDBC_OPTION_KEYWORD}inputDataPartition.count"
  val jdbcPasswordStrategy: String = s"${GIMEL_JDBC_OPTION_KEYWORD}p.strategy"
  val jdbcUpdateSetColumns: String =
    s"${GIMEL_JDBC_OPTION_KEYWORD}update.setColumns"
  val jdbcUpdateWhereColumns: String =
    s"${GIMEL_JDBC_OPTION_KEYWORD}update.whereColumns"
  val jdbcPFileSource = s"${GIMEL_JDBC_OPTION_KEYWORD}p.file.source"
  val jdbcPartitionColumns =
    s"${GIMEL_JDBC_OPTION_KEYWORD}read.partition.columns"
  val jdbcPerProcessMaxRowsThreshold =
    s"${GIMEL_JDBC_OPTION_KEYWORD}process.rows.maxlimit"
  val jdbcEnableQueryRunAsIs =
    s"${GIMEL_JDBC_OPTION_KEYWORD}process.enable.complete.pushdown"
  val jdbcCompletePushdownSelectEnabled =
    s"${GIMEL_JDBC_OPTION_KEYWORD}process.complete.pushdown.select.enabled"
  val jdbcFastLoadErrLimit =
    s"${GIMEL_JDBC_OPTION_KEYWORD}write.fastload.error.limit"
  val jdbcAuthLoaderClass =
    s"${GIMEL_JDBC_OPTION_KEYWORD}auth.provider.class"
}
