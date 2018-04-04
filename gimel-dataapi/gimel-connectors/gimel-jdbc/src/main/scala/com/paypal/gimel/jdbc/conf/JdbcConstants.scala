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
  val batchUserPFILE = s"${hdfsPrefix}/livy/password/pp_batch_opts_admin/teradata/${teradataFileName}"
  val pFile = s"${hdfsPrefix}/${GimelConstants.USER_NAME}/password/teradata/${teradataFileName}"
  val jdbcUserName: String = s"spark.jdbc.${GimelConstants.USER_NAME}"
  // polling properties
  val jdbcPPath = s"${pFile}/${teradataFileName}"
}

