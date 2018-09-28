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

package com.paypal.gimel.sftp.reader

import org.apache.spark.sql.{SparkSession}

import com.paypal.gimel.logger.Logger
import com.paypal.gimel.sftp.conf.{SFTPClientConfiguration, SFTPConfigs}

object SFTPReader {

  val logger: Logger = Logger()

  /**
    * read implementation which calls the springML read to load the file from SFTP server
    * @param sparkSession - Spark session object
    * @param conf - Set of SFTP configuration properties to connect to SFTP server
    * @return - A dataframe containing the records of the SFTP file
    */
  def read(sparkSession: SparkSession, conf: SFTPClientConfiguration): org.apache.spark.sql.DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)
    sparkSession.read.format(SFTPConfigs.sftpClass).options(conf.finalProps).load(conf.filePath)
  }
}
