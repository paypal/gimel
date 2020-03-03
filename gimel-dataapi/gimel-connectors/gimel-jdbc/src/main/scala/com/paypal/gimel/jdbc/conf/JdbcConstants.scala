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

import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.paypal.gimel.common.conf.GimelConstants

object JdbcConstants {

  // basic variable references
  val HDFS_PREFIX: String = "hdfs:///user"
  val TD_PASS_FILENAME_DEFAULT: String = "pass.dat"
  val P_FILEPATH = s"${HDFS_PREFIX}/${GimelConstants.USER_NAME}/password/teradata/${TD_PASS_FILENAME_DEFAULT}"
  val MYSQL = "MYSQL"
  val TERADATA = "TERADATA"
  val ORCALE = "ORACLE"
  val POSTGRESQL = "POSTGRESQL"

  val HDFS_FILE_SOURCE = "hdfs"
  val LOCAL_FILE_SOURCE = "local"
  val DEFAULT_P_FILE_SOURCE = HDFS_FILE_SOURCE
  val JDBC_FILE_PASSWORD_STRATEGY = "file"
  val JDBC_INLINE_PASSWORD_STRATEGY = "inline"
  val JDBC_PROXY_USERNAME = "gimelproxyuser"
  val JDBC_DEFAULT_PASSWORD_STRATEGY = JDBC_FILE_PASSWORD_STRATEGY
  val JDBC_CUSTOM_PASSWORD_STRATEGY = "custom"
  val JDBC_AUTH_REQUEST_TYPE = "JDBC"

  // default TD properties
  val DEFAULT_TD_SESSIONS = 5
  val DEFAULT_CHARSET = "UTF16"
  val DEFAULT_SESSIONS = "6"
  val TD_FASTLOAD_KEY: String = "FASTLOAD"
  val TD_FASTEXPORT_KEY: String = "FASTEXPORT"
  val TD_FASTLOAD_KEY_LC: String = TD_FASTLOAD_KEY.toLowerCase
  val TD_FASTEXPORT_KEY_LC: String = TD_FASTEXPORT_KEY.toLowerCase

  // JDBC READ configs
  val MAX_TD_JDBC_READ_PARTITIONS = 24
  val MAX_FAST_EXPORT_READ_PARTITIONS = 2
  val DEFAULT_READ_FETCH_SIZE = 1000
  val DEFAULT_LOWER_BOUND = 0
  val DEFAULT_UPPER_BOUND = 20
  val DEFAULT_READ_TYPE = "BATCH"
  val READ_OPERATION = "read"

  // JDBC write configs
  val GIMEL_TEMP_PARTITION = "GIMEL_TEMP_PARTITION"
  val DEFAULT_WRITE_BATCH_SIZE = 10000
  val MAX_TD_JDBC_WRITE_PARTITIONS: Int = 24
  val MAX_FAST_LOAD_WRITE_PARTITIONS: Int = 2
  val DEFAULT_INSERT_STRATEGY = "insert"
  val DEFAULT_WRITE_TYPE = "BATCH"
  val WRITE_OPERATION = "write"
  val REPARTITION_METHOD = "repartition"
  val COALESCE_METHOD = "coalesce"

  // partitions for Systems other than Teradata
  val DEFAULT_JDBC_READ_PARTITIONS = 100
  val DEFAULT_JDBC_WRTIE_PARTITIONS = 100
  val DEFAULT_JDBC_PER_PROCESS_MAX_ROWS_LIMIT: Long = 1000000000L
  val DEFAULT_JDBC_PER_PROCESS_MAX_ROWS_LIMIT_STRING: String = DEFAULT_JDBC_PER_PROCESS_MAX_ROWS_LIMIT.toString

  // pushdown constants
  val DEF_JDBC_PUSH_DOWN_SCHEMA: StructType = new StructType(fields = Seq(
    StructField("QUERY_EXECUTION", StringType, nullable = false)
  ).toArray)
}
