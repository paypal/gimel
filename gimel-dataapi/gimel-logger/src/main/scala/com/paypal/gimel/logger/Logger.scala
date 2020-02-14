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

package com.paypal.gimel.logger

import java.util.Calendar

import scala.collection.JavaConverters._

import com.paypal.gimel.logger.conf.LoggerConstants
import com.paypal.gimel.logging.impl.JSONSystemLogger

/**
  * com.paypal.gimel.logger.Logger SingleTon
  * Initiate New com.paypal.gimel.logger.Logger with a com.paypal.gimel.logger.Logger(Conf),
  * and from there on - get com.paypal.gimel.logger.Logger via com.paypal.gimel.logger.Logger() call
  */

object Logger {

  @transient private var instance: Logger = null

  /**
    *
    * @param conf Any Configuration Object Or just a String to uniquely identify the logging
    * @return com.paypal.gimel.logger.Logger Instance (got or Created Newly)
    */
  def apply(conf: Any): Logger = {
    if (instance == null) {
      instance = new Logger(conf)
      instance
    } else {
      instance
    }
  }

  def apply(): Logger = {
    if (instance == null) {
      val user = sys.env.getOrElse("USER", "UnknownUser")
      val tag = s"$user-Initiated-${Calendar.getInstance.getTime}"
      instance = new Logger(tag)
    }
    instance
  }
}

/**
  * Logging Implementation
  *
  * @param config A Configuration Object, it can as well be a Simple String Tag
  *
  *               Example : flights_log
  */

@SerialVersionUID(666L)
class Logger(config: Any) extends Serializable {

  private val _APP_reference = config.toString
  private val logModes = Map(4 -> "INFO", 3 -> "DEBUG", 2 -> "WARN", 1 -> "ERROR")
  @volatile private var logMode = 4
  lazy val logger: JSONSystemLogger = JSONSystemLogger.getInstance(getClass)
  private var logAudit = false
  var consolePrintEnabled = false
  // logic to attempt logging
  var auditingAndAlertingEnabled = false

  private var sparkVersion: String = null
  /**
    * Set Log Level Push to Kafka
    *
    * @param customLogAudit Boolean property to push logs to kafka
    */
  def setLogAudit(customLogAudit: Boolean = true): Unit = {
    logAudit = customLogAudit
    auditingAndAlertingEnabled = customLogAudit
  }

  /**
    * API to set Spark Version for logging
    *
    * @param sparkVersion Spark Version
    */
  def setSparkVersion(sparkVersion: String): Unit = this.sparkVersion = sparkVersion

  /**
    * Set Log Level
    *
    * @param level Level Message such as - ERROR, INFO, DEBUG, WARN
    */
  def setLogLevel(level: String): Unit = {
    val userRequestedLevel = logModes.map(x => x._2 -> x._1).get(level)
    userRequestedLevel match {
      case None =>
        warning(s"Invalid Log Level Supplied: $level. No Changes will be applied to Logging Level.")
      case _ =>
        logMode = userRequestedLevel.get
        info(s"com.paypal.gimel.logger.Logger Level to be Set at $logMode")
    }
  }

  /**
    * Silence com.paypal.gimel.logger.Logger to Print Only ERROR Messages
    */
  def silence: Unit = {
    logMode = 1
    consolePrintEnabled = false
  }

  /**
    * Log message/object as debug.
    *
    * @param message
    * @param sendToKafka
    */
  def debug(message: Any, sendToKafka: Boolean = false): Unit = {
    try {
      val finalMessage = message match {
        case null =>
          " "
        case _ =>
          if (!sendToKafka) s"[${_APP_reference}] : ${message.toString}" else message
      }
      if (logMode >= 2 && auditingAndAlertingEnabled) logger.debug(finalMessage.asInstanceOf[Object])
      if (consolePrintEnabled) println(s"GIMEL-LOGGER | ${Calendar.getInstance().getTime} | ${message}")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    }
  }

  /**
    * Log message/object as info.
    *
    * @param message
    * @param sendToKafka
    */
  def info(message: Any, sendToKafka: Boolean = false): Unit = {
    try {
      val finalMessage = message match {
        case null =>
          " "
        case _ =>
          if (!sendToKafka) s"[${_APP_reference}] : ${message.toString}" else message
      }
      if (logMode >= 4 && auditingAndAlertingEnabled) logger.info(finalMessage.asInstanceOf[Object])
      if (consolePrintEnabled) println(s"GIMEL-LOGGER | ${Calendar.getInstance().getTime} | ${message}")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    }
  }

  /**
    * Log message/object as warn.
    *
    * @param message
    * @param sendToKafka
    */
  def warning(message: Any, sendToKafka: Boolean = false): Unit = {
    try {
      val finalMessage = message match {
        case null =>
          " "
        case _ =>
          if (!sendToKafka) s"[${_APP_reference}] : ${message.toString}" else message
      }
      if (logMode >= 3 && auditingAndAlertingEnabled) logger.warn(finalMessage.asInstanceOf[Object])
      if (consolePrintEnabled) println(s"GIMEL-LOGGER | ${Calendar.getInstance().getTime} | ${message}")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    }
  }

  /**
    * Log message/object as error.
    *
    * @param message
    */
  def error(message: Any): Unit = {
    try {
      val finalMessage = message match {
        case null =>
          " "
        case _ =>
          s"[${_APP_reference}] : ${message.toString}"
      }
      if (logMode >= 1 && auditingAndAlertingEnabled) logger.error(finalMessage)
      if (consolePrintEnabled) println(s"GIMEL-LOGGER | ${Calendar.getInstance().getTime} | ${message}")
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    }
  }

  /**
    * Logging the Method Access to Kafka
    *
    * @param yarnAppId                 Yarn Application ID
    * @param yarnAppName               Yarn Application Name
    * @param runType                   Run Type is Either Batch or Stream
    * @param runMode                   Either Batch or Stream
    * @param cluster                   The Cluster name
    * @param user                      User who is running the API code
    * @param appTag                    Gimel Unique Application Tag
    * @param method                    The method, example - read, write, executeQuery
    * @param sql                       The SQL String executed by User
    * @param moreProps                 Any additional optional parameters that the Logging Api is trying to log
    * @param apiAccessCompletionStatus status of the API call
    * @param apiErrorDescription       detailed stacktrace of error occured
    * @param apiError                  Any specific errors in API
    * @return Map of all the information that is being logged.
    */
  def logMethodAccess(yarnAppId: String
                      , yarnAppName: String
                      , runType: String
                      , runMode: String
                      , cluster: String
                      , user: String
                      , appTag: String
                      , method: String
                      , sql: String = ""
                      , moreProps: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
                      , apiAccessCompletionStatus: String = LoggerConstants.UNKNOWN_STRING
                      , apiErrorDescription: String = LoggerConstants.UNKNOWN_STRING
                      , apiError: String = LoggerConstants.UNKNOWN_STRING
                     ): Map[String, Any] = {

    val additionalProps: scala.collection.mutable.Map[String, String] = moreProps.map { case (k, v) =>
      (k.replaceAllLiterally(".", "~") -> v)
    }

    val accessAuditInfo: Map[String, Any] = Map(
      "apiUser" -> user,
      "apiApiType" -> runType,
      "apiRunMode" -> runMode,
      "apiCluster" -> cluster,
      "apiAppId" -> yarnAppId,
      "apiAppName" -> yarnAppName,
      "apiAppTag" -> appTag.replaceAllLiterally("/", "_"),
      "apiAccessMethod" -> method,
      "logtime" -> System.currentTimeMillis(),
      "logType" -> "gimelDataApiMethodAudit",
      "apiVersion" -> sparkVersion,
      "apiSql" -> sql,
      "additionalProps" -> additionalProps,
      "apiAccessCompletionStatus" -> apiAccessCompletionStatus,
      "apiErrorDescription" -> apiErrorDescription,
      "apiError" -> apiError
    ) ++ moreProps

    if (logAudit) {
      this.info("Auditing Information being posted to Gimel Audit Log...")
      this.info(accessAuditInfo)
      logger.info(accessAuditInfo.asJava)
    }
    accessAuditInfo
  }

  /**
    * Logging the Method Access to Kafka
    *
    * @param yarnAppId                 Yarn Application ID
    * @param yarnAppName               Yarn Application Name
    * @param runType                   Run Type mostly the class name
    * @param runMode                   Either Batch or Stream
    * @param cluster                   The Cluster name
    * @param user                      User who is running the API code
    * @param appTag                    Gimel Unique Application Tag
    * @param method                    The method, example - read, write, executeQuery
    * @param dataSet                   Name of the dataset
    * @param systemType                Type of the System  , say Kafka, Elasticsearch , etc.
    * @param sql                       The SQL String executed by User
    * @param moreProps                 Any additional optional parameters that the Logging Api is trying to log
    * @param apiAccessCompletionStatus status of the API call
    * @param apiErrorDescription       detailed stacktrace of error occured
    * @param apiError                  Any specific errors in API
    * @param startTime                 start time of API
    * @param endTime                   endtime for API
    * @param executionTime             total execution time for API
    * @return Map of all the information that is being logged.
    */
  def logApiAccess(yarnAppId: String
                   , yarnAppName: String
                   , runType: String
                   , runMode: String
                   , cluster: String
                   , user: String
                   , appTag: String
                   , method: String
                   , dataSet: String
                   , systemType: String
                   , sql: String = ""
                   , moreProps: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
                   , apiAccessCompletionStatus: String = "unknown"
                   , apiErrorDescription: String = "unknown"
                   , apiError: String = "unknown"
                   , startTime: Long
                   , endTime: Long
                   , executionTime: Double = 0.0
                  ): Map[String, Any] = {

    val additionalProps: scala.collection.mutable.Map[String, String] = moreProps.map { case (k, v) =>
      (k.replaceAllLiterally(".", "~") -> v)
    }
    val accessAuditInfo: Map[String, Any] = Map(
      "apiUser" -> user,
      "apiApiType" -> runType,
      "apiRunMode" -> runMode,
      "apiCluster" -> cluster,
      "apiAppId" -> yarnAppId,
      "apiAppName" -> yarnAppName,
      "apiAppTag" -> appTag.replaceAllLiterally("/", "_"),
      "apiAccessType" -> method,
      "apiDataSetName" -> dataSet,
      "apiDataSetType" -> systemType,
      "logtime" -> System.currentTimeMillis(),
      "logType" -> "gimelDataApiAccessAudit",
      "apiVersion" -> sparkVersion,
      "apiSql" -> sql,
      "additionalProps" -> additionalProps,
      "apiAccessCompletionStatus" -> apiAccessCompletionStatus,
      "apiErrorDescription" -> apiErrorDescription,
      "apiError" -> apiError,
      "apiStartTime" -> startTime,
      "apiEndTime" -> endTime,
      "apiExecutionTime" -> executionTime
    ) ++ moreProps

    if (logAudit) {
      this.info("Auditing Information being posted to Gimel Audit Log...")
      this.info(accessAuditInfo)
      logger.info(accessAuditInfo.asJava)
    }

    this.logMethodAccess(yarnAppId
      , yarnAppName
      , runType
      , runMode
      , cluster
      , user
      , appTag
      , method
      , sql
      , additionalProps
      , apiAccessCompletionStatus
      , apiErrorDescription
      , apiError
    )
    accessAuditInfo
  }

  /**
    * This method prints the error on console.
    *
    * @param msg text to print
    */
  def throwError(msg: String): Unit = {
    val errorMsg =
      s"""\n|-------------------------------------------------------------------------------------------------------------------
         |${msg}
         |-------------------------------------------------------------------------------------------------------------------
          """.stripMargin

    println(s"GIMEL-ERROR | ${Calendar.getInstance().getTime} | ${errorMsg}")
  }

}
