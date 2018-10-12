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

object JdbcConstants {

  // basic variable references
  val hdfsPrefix: String = "hdfs:///user"
  val teradataFileName: String = "pass.dat"
  val pFile = s"${hdfsPrefix}/${GimelConstants.USER_NAME}/password/teradata/${teradataFileName}"
  val jdbcUserName: String = s"gimel.jdbc.${GimelConstants.USER_NAME}"
  val MYSQL = "MYSQL"
  val TERADATA = "TERADATA"

  // polling properties
  val jdbcPPath = "/etl/LVS/dmetldata11/gimel/conf/pass.dat"

  // default TD properties
  val defaultTeradataSessions = 5
  val deafaultCharset = "UTF16"
  val defaultSessions = "6"

  // JDBC READ configs
  val NUM_READ_PARTITIONS = 16
  val defaultReadFetchSize = 1000
  val defaultFastExportSessions = "16"
  val defaultLowerBound = 0
  val defaultUpperBound = 20
  val defaultReadType = "BATCH"
  val noPartitionColumn = "10"

  // JDBC write configs
  val GIMEL_TEMP_PARTITION = "GIMEL_TEMP_PARTITION"
  val defaultWriteBatchSize = 10000
  val NUM_WRITE_PARTITIONS = 12
  val defaultInsertStrategy = "insert"
  val defaultFastLoadSessions = "12"
  val defaultWriteType = "BATCH"


}

