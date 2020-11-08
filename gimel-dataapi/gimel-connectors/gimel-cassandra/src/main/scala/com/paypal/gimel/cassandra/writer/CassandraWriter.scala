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

package com.paypal.gimel.cassandra.writer

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

import com.paypal.gimel.cassandra.conf.{CassandraClientConfiguration, CassandraConfigs}
import com.paypal.gimel.logger.Logger

object CassandraWriter {

  private val logger = Logger()

  /**
    * Writes DataFrame to Cassandra Table
    *
    * @param sparkSession SparkSession
    * @param conf         CassandraClientConfiguration
    * @param dataFrame    DataFrame
    * @return DataFrame
    */
  def writeToTable(sparkSession: SparkSession, conf: CassandraClientConfiguration, dataFrame: DataFrame): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    sparkSession.setCassandraConf(conf.cassandraCluster, CassandraConnectorConf.ConnectionHostParam.option(conf.cassandraHosts))
    sparkSession.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(conf.cassandraSparkTtl))

    dataFrame.write
      .format(CassandraConfigs.gimelCassandraSparkDriver)
      .options(conf.cassandraDfOptions)
      .save()

    dataFrame
  }

}
