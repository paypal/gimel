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

import com.google.api.client.json.GenericJson
import java.util.Base64
import org.apache.commons.lang3.CharEncoding.UTF_8

import com.paypal.gimel.bigquery.conf.{BigQueryConfigs, BigQueryConstants}
import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.security.CipherUtils
import com.paypal.gimel.common.storageadmin.HDFSAdminClient
import com.paypal.gimel.logger.Logger

object BigQueryUtilities {

  val logger = Logger()

  /**
   * Check if Big Query Table name is supplied. If not supplied : fail.
   * @param bigQueryTable The Big Query Table Name in the form of "gimel.bigquery.table"
   */
  def failIfTableNotSpecified(bigQueryTable: Option[String]): Unit = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    bigQueryTable match {
      case Some(tbl) => logger.info(s"Found Table ${bigQueryTable.get}")
      case _ => throw new IllegalArgumentException(
        s"""Big Query Connector Requires Table Name. Please pass the option [${BigQueryConfigs.bigQueryTable}] in the write API.""")
    }
  }

  /**
   * Check if Big Query Compute Project name is supplied. If not supplied : fail.
   * @param bigComputeProject The Big Query Table Name in the form of "gimel.bigquery.table"
   */
  def failIfComputeProjectNotSpecified(bigComputeProject: Option[String]): Unit = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    bigComputeProject match {
      case Some(tbl) => logger.info(s"Found Compute or Parent Project ${bigComputeProject.get}")
      case _ => throw new IllegalArgumentException(
        s"""Big Query Connector Requires Parent / Compute project to be specified . Please pass the option [${BigQueryConfigs.bigQueryComputeProject}] in the write API.""")
    }
  }

  /**
   * Parses the Save Mode for the write functionality
   * @param saveMode Spark's Save mode for Sink (such as Append, Overwrite ...)
   * @param bigQueryTable The Big Query Table Name in the form of "gimel.bigquery.table"
   */
  def parseSaveMode(saveMode: String, bigQueryTable: String): Unit = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

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

  def withCred(options: Map[String, String]): Map[String, String] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val json = new GenericJson()
    val authProviderClass = options.getOrElse(BigQueryConfigs.bigQueryAuthProviderClass, GimelConstants.DEFAULT_AUTH_PROVIDER_CLASS)
    var authLoader: com.paypal.gimel.common.security.AuthProvider = null
    if (options.getOrElse(BigQueryConfigs.bigQueryAuthProviderToBeLoaded, GimelConstants.TRUE_UPPER).toUpperCase().equals(GimelConstants.TRUE_UPPER)) {
      logger.info("LOADING AUTH PROVIDER")
      authLoader = Class.forName(authProviderClass).newInstance.asInstanceOf[com.paypal.gimel.common.security.AuthProvider]
    } else logger.info("NOT LOADING AUTH PROVIDER")

    val clientIdName = options.getOrElse(BigQueryConfigs.bigQueryKmsClientIdName, BigQueryConstants.notebooksClientId)
    val clientSecretName = options.getOrElse(BigQueryConfigs.bigQuerykmsClientSecretName, BigQueryConstants.notebooksClientSecret)
    val decrKeyName = options.getOrElse(BigQueryConfigs.bigQuerykmsDecrKeyName, GimelConstants.NOTEBOOKS_KEY_MAKER_KEY)
    val requestType = Map(GimelConstants.UDC_AUTH_REQUEST_TYPE -> GimelConstants.UDC_GIMEL)
    val kmsOptionsClientId = Map(GimelConstants.GIMEL_KEY_MAKER_APP_KEY -> clientIdName) ++ requestType
    val kmsOptionsClientSecret = Map(GimelConstants.GIMEL_KEY_MAKER_APP_KEY -> clientSecretName) ++ requestType
    val kmsOptionsDecrKey = Map(GimelConstants.GIMEL_KEY_MAKER_APP_KEY -> decrKeyName) ++ requestType
    logger.info("Fetching clientId, clientSecret & decryption Key from Keymaker...")
    val clientId = authLoader.getCredentials(kmsOptionsClientId)
    logger.info("Successfully fetched - clientId")
    logger.info("Sleeping for a second to avoid being rate limited...")
    Thread.sleep(1000)
    val clientSecret = authLoader.getCredentials(kmsOptionsClientSecret)
    logger.info("Successfully fetched - clientSecret")
    logger.info("Sleeping for a second to avoid being rate limited...")
    Thread.sleep(1000)
    val privateKey = authLoader.getCredentials(kmsOptionsDecrKey)
    logger.info("Successfully fetched - privateKey")

    val rfrshTknEnc: Option[String] = options.get(BigQueryConfigs.bigQueryRefreshToken)
    logger.debug(s"REFRESH TOKEN -> ${rfrshTknEnc}")
    val rfrshTkn: String = rfrshTknEnc match {
      case Some(token) =>
        logger.debug(s"Decrypting [${rfrshTknEnc}]")
        CipherUtils.decrypt(privateKey, token)
      case None =>
        logger.info(s"Using Hardcoded Token !")
        "XXX"
    }

    json.put(GimelConstants.CLIEND_ID, clientId)
    json.put(GimelConstants.CLIENT_SECRET, clientSecret)
    json.put(GimelConstants.REFRESH_TOKEN, rfrshTkn)
    json.put(GimelConstants.TYPE, GimelConstants.AUTHORIZED_USER)
    val credKey: String = Base64.getEncoder().encodeToString(json.toString().getBytes(UTF_8))
    options ++ Map("credentials" -> credKey)
  }

  def getResolvedProperties(dataSetProps: Map[String, Any]): Map[String, String] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    if (dataSetProps.isEmpty) throw new IllegalArgumentException("Props Map Cannot be empty for BigQuery Dataset Read.")

    logger.debug("Merging DataSet Properties with RunTime options ...")
    var options: Map[String, String] = datasetProps.props ++ dataSetProps.map(x => (x._1, x._2.toString))

    logger.debug("Adding Credentials to Query parameters...")
    options ++= withCred(options)

    val bigQueryTable: Option[String] = options.get(BigQueryConfigs.bigQueryTable)
    failIfTableNotSpecified(bigQueryTable)
    logger.debug("Adding Parent Project to Query parameters ...")
    options ++= Map(BigQueryConstants.bigQueryTable -> bigQueryTable.get)

    val bigQueryParentProject: Option[String] = options.get(BigQueryConfigs.bigQueryComputeProject)
    failIfComputeProjectNotSpecified(bigQueryParentProject)
    logger.debug("Adding Parent Project to Query parameters ...")
    options ++= Map(BigQueryConstants.parentProject -> bigQueryParentProject.get)

    logger.debug(s"Resolved Parameters --> ${options}")
    options
  }

  def getTokenFromHDFS(tokeName: String, options: Map[String, String]): String = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val resolvedTokenName = s"gimel.bigquery.${tokeName.toLowerCase}.source.file"
    val sourceFile = options.get(resolvedTokenName) match {
      case Some(file) => file
      case _ => throw new Exception(s"Missing [${resolvedTokenName}] for HDFS")
    }
    HDFSAdminClient.readHDFSFile(sourceFile).replaceAllLiterally("\n", "")
  }

}
