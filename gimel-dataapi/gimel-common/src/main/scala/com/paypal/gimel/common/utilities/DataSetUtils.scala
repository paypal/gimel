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

package com.paypal.gimel.common.utilities

import org.apache.spark.SparkContext

import com.paypal.gimel.common.conf.GimelConstants

object DataSetUtils {

  /**
    * Resolves the DatasetName by adding "default" as database if user passes just table name
    *
    * @param sourceName DB.Table or just Table
    * @return DB.Table
    */
  def resolveDataSetName(sourceName: String): String = {
    if (sourceName.contains('.')) {
      sourceName
    } else {
      s"default.$sourceName"
    }
  }

  /**
    * getYarnClusterName - gets the yarn cluster from the hadoop config file
    *
    * @return
    */
  def getYarnClusterName(): String = {
    val hadoopConfiguration = new org.apache.hadoop.conf.Configuration()
    val cluster = hadoopConfiguration.get(GimelConstants.FS_DEFAULT_NAME)
    cluster.split("/").last
  }

  /**
    * Determine Supported Properties
    * Currently Supported :
    * Map[String, Any]
    * String Example """hdfs.path=/sys/datalake;hdfs.file.format=parquet;hdfs.target.compress=true"""
    *
    * @param x Properties
    * @return Map[String, Any]
    */
  def getProps(x: Any): Map[String, Any] = {
    x match {
      case map: Map[String, Any] =>
        map
      case str: String =>
        propStringToMap(str)
      case _ =>
        val examplesString =
          """|
            |hdfs.path=/sys/datalake;hdfs.file.format=parquet;hdfs.target.compress=true
            | """.stripMargin.trim
        val errorMessageForClient =
          s"""
             |Invalid props type ${x.getClass.getCanonicalName}.
             |Supported types are eitherMap[String, String] OR String.
             |Valid example for String --> $examplesString
          """.stripMargin
        throw new Exception(errorMessageForClient)
    }
  }

  /**
    * Try to Convert a Key Value Pairs Text into a Map
    *
    * @param x KV pairs | Example """hdfs.path=/sys/datalake:hdfs.file.format=parquet:hdfs.target.compress=true"""
    * @return Map[String, Any]
    */
  def propStringToMap(x: String): Map[String, String] = {
    x.split(':').flatMap { keyValuePair =>
      keyValuePair.split('=') match {
        case Array("") =>
          None
        case Array(key, value) =>
          Some(key -> value)
        case _ =>
          val examplesString =
            """
              |hdfs.path=/sys/datalake;hdfs.file.format=parquet;hdfs.target.compress=true
              | """.stripMargin.trim
          val errorMessageForClient =
            s"""
               |Error While Parsing Key=Value Pairs in $x. Unable to convert the given string to pair!
               |Valid Example --> Example --> $examplesString
          """.stripMargin
          throw new Exception(errorMessageForClient)
      }
    }.toMap
  }

  /**
    * Gives an Unique App Tag for given execution
    *
    * @param sparkContext SparkContext
    * @return Application Tag String
    */
  def getAppTag(sparkContext: SparkContext): String = {
    val user = getUserName(sparkContext)
    val sparkAppName = getSparkAppName(sparkContext)
    val clusterName = getYarnClusterName()
    val appTag = s"${clusterName}/${user}/${sparkAppName}".replaceAllLiterally(" ", "-")
    appTag
  }

  /**
    * Gives a Unique Structured Streaming checkpoint Location on hdfs
    *
    * @param sparkContext SparkContext
    * @return Checkpoint Location
    */
  def getStructuredStreamingCheckpointLocation(sparkContext: SparkContext): String = {
    val user = getUserName(sparkContext)
    val sparkAppName = getSparkAppName(sparkContext)
    val checkpointLocation = s"/user/${user}/gimel_spark_app_kafka_checkpoint/${sparkAppName}".replaceAllLiterally(" ", "-")
    checkpointLocation
  }

  /**
    * Gives User Name
    *
    * @param sparkContext SparkContext
    * @return User Name
    */
  def getUserName(sparkContext: SparkContext): String = {
    val user = sparkContext.getLocalProperty(GimelConstants.GTS_USER_CONFIG) match {
      case null => {
        sys.env("USER")
      }
      case _ => {
        sparkContext.getLocalProperty(GimelConstants.GTS_USER_CONFIG)
      }
    }
    user
  }

  /**
    * Gives Spark App Name
    *
    * @param sparkContext SparkContext
    * @return Spark App Name
    */
  def getSparkAppName(sparkContext: SparkContext): String = {
    sparkContext.getConf.get(GimelConstants.SPARK_APP_NAME)
  }
}
