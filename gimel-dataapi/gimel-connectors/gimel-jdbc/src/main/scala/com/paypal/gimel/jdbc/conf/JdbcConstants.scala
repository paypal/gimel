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
  val hdfsPrefix: String = "hdfs:///user"
  val teradataFileName: String = "pass.dat"
  val pFile = s"${hdfsPrefix}/${GimelConstants.USER_NAME}/password/teradata/${teradataFileName}"
  val jdbcUserName: String = s"gimel.jdbc.${GimelConstants.USER_NAME}"
  val MYSQL = "MYSQL"
  val TERADATA = "TERADATA"
  val ORCALE = "ORACLE"
  val POSTGRESQL = "POSTGRESQL"
  val defaultJdbcPFileSource = "hdfs"
  val localFileSource = "local"

  // polling properties
  val jdbcPPath = "default_path"
  val jdbcDefaultPasswordStrategy = "queryband"
  val jdbcFilePasswordStrategy = "file"
  val jdbcProxyUsername = "gimelproxyuser"

  // default TD properties
  val defaultTeradataSessions = 5
  val defaultCharset = "UTF16"
  val defaultSessions = "6"
  val TD_FASTLOAD_KEY: String = "FASTLOAD"
  val TD_FASTEXPORT_KEY: String = "FASTEXPORT"
  val TD_FASTLOAD_KEY_LC: String = TD_FASTLOAD_KEY.toLowerCase
  val TD_FASTEXPORT_KEY_LC: String = TD_FASTEXPORT_KEY.toLowerCase

  // INCLUDE LIST
  val dbList: String = "DATABASE_LIST"
  val includeStatus: String = "INCLUDE_STATUS"
  val deltaHours: String = "DELTA_HOURS"

  // JDBC READ configs
  val MAX_TD_JDBC_READ_PARTITIONS = 24
  val MAX_FAST_EXPORT_READ_PARTITIONS = 2
  val defaultReadFetchSize = 1000
  val defaultFastExportSessions = "16"
  val defaultLowerBound = 0
  val defaultUpperBound = 20
  val defaultReadType = "BATCH"
  val noPartitionColumn = "10"
  val readOperation = "read"


  // JDBC write configs
  val GIMEL_TEMP_PARTITION = "GIMEL_TEMP_PARTITION"
  val defaultWriteBatchSize = 10000
  val MAX_TD_JDBC_WRITE_PARTITIONS: Int = 24
  val MAX_FAST_LOAD_WRITE_PARTITIONS: Int = 2
  val defaultInsertStrategy = "insert"
  val defaultFastLoadSessions = "12"
  val defaultWriteType = "BATCH"
  val writeOperation = "write"
  val defaultPartitionMethod = "coalesce"
  val repartitionMethod = "repartition"
  val coalesceMethod = "coalesce"

  // partitions for Systems other than Teradata
  val defaultJDBCReadPartitions = 100
  val defaultJDBCWritePartitions = 100
  val defaultJDBCPerProcessMaxRowsLimit: Long = 1000000000L
  val defaultJDBCPerProcessMaxRowsLimitString: String = defaultJDBCPerProcessMaxRowsLimit.toString
  val defaultJDBCProcesEnableRunasis: String = "false"
  val defaultJDBCProcesEnableRunasisB: Boolean = defaultJDBCProcesEnableRunasis.toBoolean

  // pushdown constants
  val DEF_JDBC_PUSH_DOWN_SCHEMA: StructType = new StructType(fields = Seq(
    StructField("QUERY_EXECUTION", StringType, nullable = false)
  ).toArray)

}
