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

package com.paypal.gimel.bigquery.utilities

import com.paypal.gimel.bigquery.conf.{BigQueryConfigs, BigQueryConstants}
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.logger.Logger

object BigQueryUtilities {

  val logger = Logger()

  /**
   * Check if Big Query Table name is supplied. If not supplied : fail.
   * @param bigQueryTable The Big Query Table Name in the form of "gimel.bigquery.table"
   */
  def failIfTableNotSpecified(bigQueryTable: Option[String]): Unit = {
    bigQueryTable match {
      case Some(tbl) => logger.info(s"Found Table ${bigQueryTable.get}")
      case _ => throw new IllegalArgumentException(
        s"""Big Query Connector Requires Table Name. Please pass the option [${BigQueryConfigs.bigQueryTable}] in the write API.""")
    }
  }

  /**
   * Parses the Save Mode for the write functionality
   * @param saveMode Spark's Save mode for Sink (such as Append, Overwrite ...)
   * @param bigQueryTable The Big Query Table Name in the form of "gimel.bigquery.table"
   */
  def parseSaveMode(saveMode: String, bigQueryTable: String): Unit = {
    saveMode match {
      case BigQueryConstants.saveModeAppend =>
        logger.info(s"Appending to Big Query table [${bigQueryTable}]")
      case BigQueryConstants.saveModeErrorIfExists =>
        logger.info(s"Asked to Err if exists Big Query table [${bigQueryTable}]")
      case BigQueryConstants.saveModeOverwrite =>
        logger.info(s"Overwriting Big Query table [${bigQueryTable}]")
      case BigQueryConstants.saveModeIgnore =>
        logger.info(s"Asked to Ignore if exists Big Query table [${bigQueryTable}]")
      case _ => throw new IllegalArgumentException(
        s"""Illegal [saveMode]:[${saveMode}]
           |${BigQueryConstants.bigQueryDocUrl}
           |""".stripMargin)
    }

  }

}
