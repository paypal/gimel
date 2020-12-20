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

package com.paypal.gimel.aerospike.utilities

import com.osscube.spark.aerospike.rdd._
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.logger.Logger

/**
  * Aerospark connector for reading data from aerospike
  *
  */

object AeroSparkConnector {
  val logger = Logger()

  /**
    *
    * @param sparkSession : SparkSession
    * @param host         Aerospike Seed host
    * @param port         Aerospike client port
    * @param namespace    Namespace name
    * @param setName      Name of the set
    * @return DataFrame
    */
  def read(sparkSession: SparkSession, host: String, port: String, namespace: String, setName: String): DataFrame = {
    try {
      val query = s"""select * from $namespace.$setName"""
      sparkSession.sqlContext.aeroRDD(s"""$host:$port""", query)
    } catch {
      case ex: Throwable =>
        logger.error(s"Failed While Reading data with Aerospark. ${ex.getStackTraceString}")
        throw ex
    }
  }
}
