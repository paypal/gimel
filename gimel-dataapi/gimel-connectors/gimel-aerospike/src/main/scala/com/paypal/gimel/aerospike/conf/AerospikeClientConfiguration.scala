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

package com.paypal.gimel.aerospike.conf

import scala.collection.immutable.Map
import scala.language.implicitConversions

import com.paypal.gimel.aerospike.utilities.AerospikeUtilities.AerospikeDataSetException
import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{GimelConstants, GimelProperties}
import com.paypal.gimel.logger.Logger

/**
  * Gimel Client Configuration for Aerospike Dataset Operations.
  *
  * @param props Aerospike Client properties.
  */

class AerospikeClientConfiguration(val props: Map[String, Any]) {

  private val logger = Logger()
  logger.info(s"Begin Building --> ${this.getClass.getName}")

  // Load Default Prop from Resource File
  val pcatProps = GimelProperties()

  // appTag is used to maintain checkpoints & various other factors that are unique to the application
  val appTag: String = props.getOrElse(GimelConstants.APP_TAG, "").toString

  // This is the DataSet Properties
  val datasetProps: DataSetProperties = props(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  val tableProps: Map[String, String] = datasetProps.props

  // Aerospike Props
  val aerospikeSeedHosts: String = props.getOrElse(AerospikeConfigs.aerospikeSeedHostsKey, tableProps.getOrElse(AerospikeConfigs.aerospikeSeedHostsKey, "")).toString
  val aerospikePort: String = props.getOrElse(AerospikeConfigs.aerospikePortKey, tableProps.getOrElse(AerospikeConfigs.aerospikePortKey, 3000.toString)).toString
  val aerospikeNamespace: String = props.getOrElse(AerospikeConfigs.aerospikeNamespaceKey, tableProps.getOrElse(AerospikeConfigs.aerospikeNamespaceKey, "test")).toString
  val aerospikeSet: String = props.getOrElse(AerospikeConfigs.aerospikeSetKey, tableProps.getOrElse(AerospikeConfigs.aerospikeSetKey, "")).toString
  val aerospikeRowKey: String = props.getOrElse(AerospikeConfigs.aerospikeRowkeyKey, tableProps.getOrElse(AerospikeConfigs.aerospikeRowkeyKey, "")).toString

  validateAerospikeProperties()
  logger.info(s"Fields Initiated --> ${this.getClass.getFields.map(f => s"${f.getName} --> ${f.get().toString}").mkString("\n")}")
  logger.info(s"Completed Building --> ${this.getClass.getName}")


  /**
    * This function validates the properties of aerospike dataset
    *
    */
  def validateAerospikeProperties(): Unit = {
    if (aerospikeSeedHosts.isEmpty) {
      logger.error("Aerospike Seed Hosts not found.")
      throw AerospikeDataSetException("Aerospike Seed Hosts not found.")
    }
    if (aerospikeNamespace.isEmpty) {
      logger.error("Aerospike namespace not found.")
      throw AerospikeDataSetException("Aerospike namespace not found.")
    }
    if (aerospikeSet.isEmpty) {
      logger.error("Aerospike Set Name not found.")
      throw AerospikeDataSetException("Aerospike Set Name not found.")
    }
  }
}
