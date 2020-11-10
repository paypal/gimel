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

package com.paypal.gimel.aerospike.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.aerospike.conf.AerospikeClientConfiguration
import com.paypal.gimel.aerospike.utilities.AeroSparkConnector
import com.paypal.gimel.aerospike.utilities.AerospikeUtilities.AerospikeDataSetException
import com.paypal.gimel.logger.Logger

/**
  * Reader for Aerospike
  *
  */

object AerospikeReader {
  val logger = Logger()

  /**
    *
    * @param sparkSession: SparkSession
    * @param dataset     Dataset name
    * @param conf        AerospikeClientConfiguration
    * @return DataFrame
    */
  def read(sparkSession: SparkSession, dataset: String, conf: AerospikeClientConfiguration): DataFrame = {
    try {
      AeroSparkConnector.read(sparkSession, conf.aerospikeSeedHosts, conf.aerospikePort, conf.aerospikeNamespace, conf.aerospikeSet)
    } catch {
      case e: Throwable =>
        throw AerospikeDataSetException(s"Error reading from Aerospike. ${e.getStackTraceString}")
    }
  }
}
