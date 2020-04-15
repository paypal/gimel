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

package com.paypal.gimel.common.conf

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object QueryGuardConfigs {
  val JOB_TTL: String = "gimel.sql.query.guard.timeout.sec"
  val DELAY_TTL: String = "gimel.sql.query.guard.delay.timeout.sec"
}
object QueryGuardConstants {
  val DEFAULT_JOB_TTL: Int = 2100000
  val DEFAULT_DELAY_TTL: Int = 10000
//  val DEFAULT_JOB_TTL: Int = 60000
//  val DEFAULT_DELAY_TTL: Int = 1000
  val EXCEPTION_MSG_FORMAT: String =
    """Gimel Query Guard Exception : Your SQL has exceeded the maximum limit of %s.
                                       | Query Start Time : %s
                                       | Query Abort Time : %s
                                       | Please start a dedicated Spark Kernel with Gimel to run long running queries.
                                       | Please tune your SQL to pull only required data by applying right filters, this will also help minimize the runtime & process only required data from source system.""".stripMargin
  val LOCAL_DATETIME_FORMATTER: DateTimeFormatter =
    DateTimeFormat.fullDateTime()
}
