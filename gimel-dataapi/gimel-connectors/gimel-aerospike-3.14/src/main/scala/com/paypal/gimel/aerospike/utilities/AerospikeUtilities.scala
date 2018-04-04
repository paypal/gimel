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

import com.aerospike.client._
import org.apache.spark.sql._

import com.paypal.gimel.aerospike.conf.AerospikeClientConfiguration
import com.paypal.gimel.common.storageadmin.AerospikeAdminClient
import com.paypal.gimel.logger.Logger

/**
  * Utilities for Aerospike Dataset
  */

object AerospikeUtilities {
  val logger = Logger()

  /**
    * This function performs write into aerospike table
    *
    * @param dataset   Name of the PCatalog Data Set
    * @param dataFrame The Dataframe to write into Target
    * @param conf
    *                  Example Usecase : Specify column name which will be used as aerospike row key
    *                  val props = Map("gimel.aerospike.rowkey" -> id)
    *                  Dataset(sc).write("aerospike_dataset", clientDataFrame,props)
    * @return DataFrame
    */
  def write(dataset: String, dataFrame: DataFrame, conf: AerospikeClientConfiguration): DataFrame = {
    try {
      val columns = dataFrame.columns.toSeq
      if (conf.aerospikeRowKey.isEmpty) {
        logger.error("Row key not found.")
        throw AerospikeDataSetException("Row key not found.")
      }
      val aerospikeHosts = conf.aerospikeSeedHosts
      val aerospikePort = conf.aerospikePort
      val aerospikeNamespace = conf.aerospikeNamespace
      val aerospikeSet = conf.aerospikeSet
      val aerospikeRowKey = conf.aerospikeRowKey
      // For each partition of Dataframe, aerospike client is created and is used to write data to aerospike
      dataFrame.foreachPartition { partition =>
        val client = AerospikeAdminClient.createClientConnection(aerospikeHosts, aerospikePort.toInt)
        partition.foreach { row =>
          val bins = columns.map(eachCol => new Bin(eachCol.toString, row.getAs(eachCol).toString)).toArray
          AerospikeAdminClient.put(client, client.asyncWritePolicyDefault, new Key(aerospikeNamespace, aerospikeSet, row.getAs(aerospikeRowKey).toString), bins)
        }
      }
      dataFrame
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw AerospikeDataSetException("Error writing data to Aerospike.")
    }
  }

  case class AerospikeDataSetException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)

}
