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
package com.paypal.gimel.sftp.conf

import scala.collection.immutable.Map

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{GimelConstants, GimelProperties}
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.sftp.utilities.SFTPUtilities

class SFTPClientConfiguration(val props: Map[String, Any]) {

  private val logger = Logger()
  logger.info(s"Begin Building --> ${this.getClass.getName}")
  logger.info(s"Incoming Properties --> ${props.map(x => s"${x._1} -> ${x._2}").mkString("\n")}")

  val dataSetProps: DataSetProperties = props(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  val tableProps: scala.collection.immutable.Map[String, String] = dataSetProps.props
  val allProps: Map[String, String] = tableProps ++ props.map { x => (x._1, x._2.toString) }
  val filePath: String = props.getOrElse(SFTPConfigs.filePath, tableProps.getOrElse(SFTPConfigs.filePath, "")).toString

  // Password Strategy will be a) user b) file c) batch
  // If it file, passwordFileSource will tell whether to read from hdfs or local
  val password: String = tableProps.getOrElse(SFTPConfigs.passwordStrategy, SFTPConstants.fileStrategy) match {
    case SFTPConstants.fileStrategy => {
      tableProps.getOrElse(SFTPConfigs.passwordFileSource, props.get(SFTPConfigs.passwordFileSource).get).toString match {
        case SFTPConstants.HDFSPasswordFile => SFTPUtilities.getPasswordFromHDFS(tableProps.getOrElse(SFTPConfigs.passwordFilePath, props.get(SFTPConfigs.passwordFilePath).get).toString)
        case SFTPConstants.localPasswordFile => SFTPUtilities.getPasswordFromLocal(tableProps.getOrElse(SFTPConfigs.passwordFilePath, props.get(SFTPConfigs.passwordFilePath).get).toString)
      }
    }
    case SFTPConstants.userStrategy => tableProps.getOrElse(SFTPConfigs.password, props.get(SFTPConfigs.password)).toString
    case SFTPConstants.batchStrategy => SFTPConstants.passwordLess
  }
  val finalProps: Map[String, String] = allProps ++ Map(
    SFTPConstants.host -> props.getOrElse(SFTPConfigs.sftpHost, tableProps.get(SFTPConfigs.sftpHost).get).toString,
    SFTPConstants.username -> props.getOrElse(SFTPConfigs.sftpUserName, tableProps.getOrElse(SFTPConfigs.sftpUserName, "")).toString,
    SFTPConstants.password -> password,
    SFTPConstants.filetype -> props.getOrElse(SFTPConfigs.fileType, tableProps.getOrElse(SFTPConfigs.fileType, "")).toString,
    SFTPConstants.delimiter -> props.getOrElse(SFTPConfigs.delimiter, tableProps.getOrElse(SFTPConfigs.delimiter, ",")).toString,
    SFTPConstants.inferschema -> props.getOrElse(SFTPConfigs.inferSchema, tableProps.getOrElse(SFTPConfigs.inferSchema, "false")).toString)

  logger.info("Final Props are " + finalProps)
}
