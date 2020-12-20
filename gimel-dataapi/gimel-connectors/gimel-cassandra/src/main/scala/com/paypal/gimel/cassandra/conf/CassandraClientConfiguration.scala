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

package com.paypal.gimel.cassandra.conf

import scala.collection.immutable.Map
import scala.language.implicitConversions

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.conf.GimelProperties
import com.paypal.gimel.logger.Logger

/**
  * Gimel Client Configuration for Cassandra Dataset Operations.
  *
  * @param props Cassandra Client properties.
  */
class CassandraClientConfiguration(val props: Map[String, Any]) {
  private val logger = Logger()
  logger.info(s"Begin Building --> ${this.getClass.getName}")
  logger.debug(s"Incoming Properties --> ${props.map(x => s"${x._1} -> ${x._2}").mkString("\n")}")

  // Load Default Prop from Resource File
  val pcatProps = GimelProperties()

  // appTag is used to maintain checkpoints & other factors that are unique to the application.
  val appTag: String = props.getOrElse(GimelConstants.APP_TAG, "").toString

  // This is the DataSet Properties
  val datasetProps: DataSetProperties = props(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  val tableProps: Map[String, String] = datasetProps.props

  logger.info(s"Hive Table Props --> ${tableProps.map(x => s"${x._1} --> ${x._2}").mkString("\n")}")

  private def errorIfMissing(key: String): Unit = {
    if (tableProps.get(key).isEmpty) {
      throw new Exception(s"Missing $key!")
    }
  }

  errorIfMissing(CassandraConfigs.gimelCassandraConnectionHosts)
  errorIfMissing(CassandraConfigs.gimelCassandraClusterName)
  errorIfMissing(CassandraConfigs.gimelCassandraKeySpaceName)
  errorIfMissing(CassandraConfigs.gimelCassandraTableName)

  private def getOrDefault(key: String, defaultVal: String = null): String = {
    if (defaultVal == null) {
      props.getOrElse(key, tableProps(key)).toString
    } else {
      props.getOrElse(key, tableProps.getOrElse(key, defaultVal)).toString
    }
  }

  val cassandraHosts = getOrDefault(CassandraConfigs.gimelCassandraConnectionHosts)
  val cassandraCluster = getOrDefault(CassandraConfigs.gimelCassandraClusterName)
  val cassandraKeySpace = getOrDefault(CassandraConfigs.gimelCassandraKeySpaceName)
  val cassandraTable = getOrDefault(CassandraConfigs.gimelCassandraTableName)
  val cassandraPushDownIsEnabled =
    getOrDefault(CassandraConfigs.gimelCassandraPushDownIsEnabled, "true").toBoolean
  val cassandraTruncateTableIsEnabled =
    getOrDefault(CassandraConfigs.gimelCassandraTableConfirmTruncate, "false").toBoolean
  val cassandraSparkInputSizeMb = getOrDefault(CassandraConfigs.gimelCassandraInputSize, "256").toInt
  val cassandraSparkTtl = getOrDefault(CassandraConfigs.gimelCassandraSparkTtl, "3600")

  val cassandraDfOptions: Map[String, String] = Map(
    "cluster" -> cassandraCluster
    , "keyspace" -> cassandraKeySpace
    , "table" -> cassandraTable
    , "pushdown" -> cassandraPushDownIsEnabled.toString
    , CassandraConfigs.gimelCassandraInputSize -> cassandraSparkInputSizeMb.toString
  )

}
