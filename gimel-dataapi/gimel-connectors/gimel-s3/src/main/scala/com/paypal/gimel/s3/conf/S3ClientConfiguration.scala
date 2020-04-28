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

package com.paypal.gimel.s3.conf

import scala.collection.immutable.Map

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{GimelConstants, GimelProperties}
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.s3.utilities.S3Utilities

class S3ClientConfiguration(val props: Map[String, Any]) {

  private val logger = Logger()

  logger.info(s"Begin Building --> ${this.getClass.getName}")
  logger.info(s"Incoming Properties --> ${props.map(x => s"${x._1} -> ${x._2}").mkString("\n")}")

  val dataSetProps: DataSetProperties = props(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  val tableProps: scala.collection.immutable.Map[String, String] = dataSetProps.props
  val allProps: Map[String, String] = tableProps ++ props.map { x => (x._1, x._2.toString) }

  val objectPath: String = props.getOrElse(S3Configs.objectPath, tableProps.getOrElse(S3Configs.objectPath, "")).toString
  if(objectPath.isEmpty) {
    throw new Exception("Object Path cannot be empty. Set the property " + S3Configs.objectPath + " !!")
  }

  val (accessId, secretKey) = props.getOrElse(S3Configs.credentialsStrategy, tableProps.getOrElse(S3Configs.credentialsStrategy, S3Constants.fileStrategy)) match {
    case S3Constants.fileStrategy => {
      val credentialsFileSource = props.getOrElse(S3Configs.credentialsFileSource, tableProps.getOrElse(S3Configs.credentialsFileSource, "")).toString
      if(credentialsFileSource.isEmpty) {
        throw new Exception("Credentials File Source cannot be empty. Set the property " + S3Configs.credentialsFileSource + " !!")
      }
      val credentialsFilePath = props.getOrElse(S3Configs.credentialsFilePath, tableProps.getOrElse(S3Configs.credentialsFilePath, "")).toString
      if(credentialsFilePath.isEmpty) {
        throw new Exception("Credentials File Path cannot be empty. Set the property " + S3Configs.credentialsFilePath + " !!")
      }
      credentialsFileSource match {
        case S3Constants.HDFSCredentialsFile => S3Utilities.getCredentialsFromHDFS(credentialsFilePath)
        case S3Constants.localCredentialsFile => S3Utilities.getCredentialsFromLocal(credentialsFilePath)
      }
    }
    case S3Constants.userStrategy => (props.getOrElse(S3Configs.accessId, tableProps.getOrElse(S3Configs.accessId, "")).toString, props.getOrElse(S3Configs.secretKey, tableProps.getOrElse(S3Configs.secretKey, "")).toString)
    case _ => (S3Constants.credentialLess, S3Constants.credentialLess)
  }

  val s3aImpl = props.getOrElse(S3Configs.s3aClientImpl, tableProps.getOrElse(S3Configs.s3aClientImpl, S3Constants.s3aImpl)).toString
  val sslEnabled = props.getOrElse(S3Configs.sslEnabled, tableProps.getOrElse(S3Configs.sslEnabled, "false")).toString
  val objectFormat = props.getOrElse(S3Configs.objectFormat, tableProps.getOrElse(S3Configs.objectFormat, "text"))
  val pathStyleAccess = props.getOrElse(S3Configs.pathStyleAccess, tableProps.getOrElse(S3Configs.pathStyleAccess, "true")).toString
  val endPoint = props.getOrElse(S3Configs.endPoint, tableProps.getOrElse(S3Configs.endPoint, "")).toString
  val delimiter = props.getOrElse(S3Configs.delimiter, tableProps.getOrElse(S3Configs.delimiter, ",")).toString
  val inferSchema = props.getOrElse(S3Configs.inferSchema, tableProps.getOrElse(S3Configs.inferSchema, "false")).toString
  val header = props.getOrElse(S3Configs.header, tableProps.getOrElse(S3Configs.header, "false")).toString
  val saveMode = props.getOrElse(S3Configs.saveMode, tableProps.getOrElse(S3Configs.saveMode, "error")).toString

  val finalProps: Map[String, String] = allProps ++ Map(
    S3Constants.delimiter -> props.getOrElse(S3Configs.delimiter, tableProps.getOrElse(S3Configs.delimiter, ",")).toString,
    S3Constants.inferschema -> props.getOrElse(S3Configs.inferSchema, tableProps.getOrElse(S3Configs.inferSchema, "false")).toString,
    S3Constants.header -> props.getOrElse(S3Configs.header, tableProps.getOrElse(S3Configs.header, "false")).toString)
}
