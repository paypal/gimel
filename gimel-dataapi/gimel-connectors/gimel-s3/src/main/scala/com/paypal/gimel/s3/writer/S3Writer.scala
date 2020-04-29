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

package com.paypal.gimel.s3.writer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import com.paypal.gimel.logger.Logger
import com.paypal.gimel.s3.conf.{S3ClientConfiguration, S3Configs, S3Constants}

object S3Writer {
  val logger: Logger = Logger()

  /**
    * Write implementation
    * @param sparkSession - Spark session object
    * @param conf - Set of S3 configuration properties to connect to S3
    * @return - A dataframe containing the records of the S3 object
    */
  def write(sparkSession: SparkSession, dataFrame: DataFrame, conf: S3ClientConfiguration): org.apache.spark.sql.DataFrame = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    System.setProperty(S3Configs.awsServicesEnableV4, "true")

    sparkSession.conf.set(S3Configs.accessId, conf.accessId)
    sparkSession.conf.set(S3Configs.secretKey, conf.secretKey)
    sparkSession.conf.set(S3Configs.s3aClientImpl, conf.s3aImpl)
    sparkSession.conf.set(S3Configs.sslEnabled, conf.sslEnabled)
    sparkSession.conf.set(S3Configs.endPoint, conf.endPoint)
    sparkSession.conf.set(S3Configs.pathStyleAccess, conf.pathStyleAccess)

    val saveMode = conf.saveMode.toLowerCase() match {
      case S3Constants.appendSaveMode =>
        SaveMode.Append
      case S3Constants.overwriteSaveMode =>
        SaveMode.Overwrite
      case S3Constants.ignoreSaveMode =>
        SaveMode.Ignore
      case _ =>
        SaveMode.ErrorIfExists
    }

    conf.objectFormat match {
      case S3Constants.csvFileFormat =>
        dataFrame.write.mode(saveMode)
          .option(S3Constants.delimiter, conf.delimiter)
          .option(S3Constants.inferschema, conf.inferSchema)
          .option(S3Constants.header, conf.header)
          .csv(conf.objectPath)
      case S3Constants.jsonFileformat =>
        dataFrame.write.mode(saveMode).json(conf.objectPath)
      case S3Constants.parquetFileFormat =>
        dataFrame.write.mode(saveMode).parquet((conf.objectPath))
      case _ =>
        dataFrame.write.mode(saveMode).text(conf.objectPath)
    }

    dataFrame
  }
}
