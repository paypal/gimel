/*
 * Copyright 2018 PayPal Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.paypal.gimel.logging

import java.util.{Date, Map => JMap}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import org.json4s.DefaultFormats

import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.scheduler.{
  StreamingListener,
  StreamingListenerBatchStarted,
  StreamingListenerBatchCompleted
}

/**
  * A {{SparkListener}} that captures and logs all metrics
  *
  * @param conf
  */
class GimelStreamingListener(conf: SparkConf) extends StreamingListener with Logging {

  def accumulateCrossJobs: Boolean = false

  private implicit def formats = DefaultFormats

  val DEFAULT_GROUP_ID: String = "DEFAULT_GROUP_ID"

  var appNotStarted: Boolean = true

  /**
    * Cluster and HostName
    */

  private val hadoopConfiguration = new org.apache.hadoop.conf.Configuration()
  val clusterUrl: String = hadoopConfiguration.get("fs.default.name")
  val clusterName: String = new java.net.URI(clusterUrl).getHost()
  val hostName: String = clusterName

  /**
    * Application Level Metrics
    */

  var appName: String = "Unknown"
  var appId: String = "Unknown"
  var driverLogs: String = "Unknown"
  var appStartTime: Long = 0L
  var appEndTime: Long = 0L
  var appElapsedTimeInSecs: Float = 0.0f
  var sparkLogLevel: String = "application"
  var sparkWebUI: String = ""
  var sparkEnvProperties: String = ""
  var sparkUser: String = ""
  var sparkVersion: String = ""
  var sparkMaster: String = ""
  var sparkDriverMemory: Long = 0L
  var sparkExecutorMemory: Long = 0L
  var sparkExecutorCores: Long = 0L
  var sparkExecutorInstances: Long = 0L

  /**
    * Job Level Metrics
    */
  var sparkJobId: Long = 0L
  var jobCompleted: Boolean = false
  var jobStartTime: Long = 0L
  var jobEndTime: Long = 0L
  var jobElapsedTimeInSecs: Float = 0.0f
  var jobSuccess: Long = 0L
  var jobFailure: Long = 0L
  var sparkJobResult: String = ""
  var jobSuccessStatus: String = ""
  var jobErrorMessage: String = ""
  var jobErrorValue: String = ""
  var jobErrorTrace: String = ""

  /**
    * Application Level Metrics
    */
  var appNumRecords = 0L
  var appProcessingDelay = 0L
  var appSchedulingDelay = 0L
  var appTotalDelay = 0L

  /**
    * Job Level Metrics
    */
  var numRecords: Long = 0L
  var processingDelay: Long = 0L
  var schedulingDelay: Long = 0L
  var totalDelay: Long = 0L

  /**
    * Generate Timestamp in YYYYMMDDHHMISS format
    */
  val dateTimeFormat = new java.text.SimpleDateFormat("yyyyMMddhhmmss")

  def timeStamp: Long = {
    dateTimeFormat.format(new Date()).toLong
  }

  /**
    * Generate Date in YYYYMMDD format
    */
  val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")

  def date: Long = {
    dateFormat.format(new Date()).toLong
  }

  /**
    * Convert to bytes
    */
  def sizeStrToBytes(str: String): Long = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      lower.substring(0, lower.length - 1).toLong * 1024
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length - 1).toLong * 1024 * 1024
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length - 1).toLong * 1024 * 1024 * 1024
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length - 1).toLong * 1024 * 1024 * 1024 * 1024
    } else {
      // no suffix, so it's just a number in bytes
      lower.toLong
    }
  }

  /**
    * Metrics that do not change in application should be set here. E.g. {{username}}
    */
  def initMetrics: Unit = {
    sparkUser = Try {
      conf.get("spark.app.user")
    } match {
      case Success(prop) => prop.toString
      case Failure(_) => "Unknown"
    }
    appName = Try {
      conf.get("spark.app.name")
    } match {
      case Success(prop) => prop.toString
      case Failure(_) => "Unknown"
    }
    appId = Try {
      conf.get("spark.app.id")
    } match {
      case Success(prop) => prop.toString
      case Failure(_) => "Unknown"
    }
    sparkVersion = Try {
      conf.get("spark.version")
    } match {
      case Success(prop) => prop.toString
      case Failure(strVal) => "Unknown"
    }
    sparkMaster = Try {
      conf.get("spark.master")
    } match {
      case Success(prop) => prop.toString
      case Failure(_) => "Unknown"
    }
    sparkDriverMemory = Try {
      conf.get("spark.driver.memory")
    } match {
      case Success(prop) => sizeStrToBytes(prop.toString)
      case Failure(_) => 0L
    }
    sparkExecutorMemory = Try {
      conf.get("spark.executor.memory")
    } match {
      case Success(prop) => sizeStrToBytes(prop.toString)
      case Failure(_) => 0L
    }
    sparkExecutorCores = Try {
      conf.get("spark.executor.cores")
    } match {
      case Success(prop) => prop.toLong
      case Failure(_) => 0L
    }
    sparkExecutorInstances = Try {
      conf.get("spark.executor.instances")
    } match {
      case Success(prop) => prop.toLong
      case Failure(_) => 0L
    }
    sparkEnvProperties = Try {
      conf.get("spark.env.properties")
    } match {
      case Success(prop) => prop.toString
      case Failure(_) => "Unknown"
    }
    sparkWebUI = Try {
      conf.get("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
    } match {
      case Success(prop) => prop.toString
      case Failure(_) => "Unknown"
    }
    sparkLogLevel = Try {
      conf.get("spark.gimel.log.level")
    } match {
      case Success(prop) => prop.toString.toLowerCase
      case Failure(_) => "application"
    }
    if ((sparkLogLevel != "application") && (sparkLogLevel != "job")) {
      println("Invalid sparkLogLevel (" + sparkLogLevel + "). Valid options: application or job. So, setting sparkLogLevel to application.")
      sparkLogLevel = "application"
    }
  }

  /**
    * Accumulate Job Level Metrics after each job completion to compute Application Level Metrics.
    */
  def accumMetrics: Unit = {
    appNumRecords += numRecords
    appProcessingDelay += processingDelay
    appSchedulingDelay += schedulingDelay
    appTotalDelay += totalDelay
  }

  /**
    * Reset Job Level Metrics after each job completion.
    */
  def resetMetrics: Unit = {
    sparkJobId = 0L
    sparkJobResult = ""
    jobSuccess = 0L
    jobFailure = 0L
    jobStartTime = 0L
    jobEndTime = 0L
    jobElapsedTimeInSecs = 0.0f
    jobSuccessStatus = ""
    jobErrorMessage = ""
    jobErrorValue = ""
    jobErrorTrace = ""
    numRecords = 0L
    processingDelay = 0L
    schedulingDelay = 0L
    totalDelay = 0L
  }

  /**
    * Log all the metrics both in JSON format and into Kafka
    *
    */

  def logJobMetrics: Unit = {
    kafkaLogger.info(this.jobKafkaArgs)
  }

  def jobKafkaArgs: JMap[String, Any] = {
    Map(

      // App Level Config Details
      "sparkVersion" -> sparkVersion,
      "logtime" -> java.lang.System.currentTimeMillis(),
      "logType" -> "LivyMetrics",
      "sparkLogLevel" -> "Streaming",
      "host" -> hostName,
      "cluster" -> clusterName,
      "appName" -> appName,
      "appId" -> appId,
      "sparkMaster" -> sparkMaster,
      "sparkUser" -> sparkUser,
      "sparkDriverMemory" -> sparkDriverMemory,
      "sparkExecutorMemory" -> sparkExecutorMemory,
      "sparkExecutorCores" -> sparkExecutorCores,
      "sparkExecutorCores" -> sparkExecutorCores,
      "sparkExecutorInstances" -> sparkExecutorInstances,
      "sparkWebUI" -> sparkWebUI,

      // App or Job Level Metrics
      "appNumRecords" -> appNumRecords,
      "appProcessingDelay" -> appProcessingDelay,
      "appSchedulingDelay" -> appSchedulingDelay,
      "appTotalDelay" -> appTotalDelay,

      // Job Level Config Details
      "sparkJobId" -> sparkJobId,
      "sparkJobResult" -> sparkJobResult,
      "jobStartTime" -> jobStartTime,
      "jobEndTime" -> jobEndTime,
      "jobElapsedTimeInSecs" -> jobElapsedTimeInSecs,
      "numRecords" -> numRecords,
      "processingDelay" -> processingDelay,
      "schedulingDelay" -> schedulingDelay,
      "totalDelay" -> totalDelay
    ).asJava
  }

  def printMetrics: Unit = {
    println("CUSTOM_LISTENER: sparkVersion = " + sparkVersion)
    println("CUSTOM_LISTENER: logdate = " + date.toString)
    println("CUSTOM_LISTENER: logtime = " + timeStamp.toString)
    println("CUSTOM_LISTENER: host = " + hostName.toString)
    println("CUSTOM_LISTENER: cluster = " + clusterName.toString)
    println("CUSTOM_LISTENER: appName = " + appName.toString)
    println("CUSTOM_LISTENER: appId = " + appId.toString)
    println("CUSTOM_LISTENER: sparkMaster = " + sparkMaster.toString)
    println("CUSTOM_LISTENER: sparkUser = " + sparkUser.toString)
    println("CUSTOM_LISTENER: sparkEnvProperties = " + sparkEnvProperties.toString)
    println("CUSTOM_LISTENER: sparkLogLevel = " + sparkLogLevel.toString)
    println("CUSTOM_LISTENER: sparkDriverMemory = " + sparkDriverMemory.toString)
    println("CUSTOM_LISTENER: sparkExecutorMemory = " + sparkExecutorMemory.toString)
    println("CUSTOM_LISTENER: sparkExecutorCores = " + sparkExecutorCores.toString)
    println("CUSTOM_LISTENER: sparkExecutorInstances = " + sparkExecutorInstances.toString)
    println("CUSTOM_LISTENER: sparkWebUI = " + sparkWebUI.toString)
    println("CUSTOM_LISTENER: appNumRecords = " + appNumRecords.toString)
    println("CUSTOM_LISTENER: appProcessingDelay = " + appProcessingDelay.toString)
    println("CUSTOM_LISTENER: appSchedulingDelay = " + appSchedulingDelay.toString)
    println("CUSTOM_LISTENER: appTotalDelay = " + appTotalDelay.toString)
    println("CUSTOM_LISTENER: sparkJobId = " + sparkJobId.toString)
    println("CUSTOM_LISTENER: sparkJobResult = " + sparkJobResult.toString)
    println("CUSTOM_LISTENER: jobStartTime = " + jobStartTime.toString)
    println("CUSTOM_LISTENER: jobEndTime = " + jobEndTime.toString)
    println("CUSTOM_LISTENER: jobElapsedTimeInSecs = " + jobElapsedTimeInSecs.toString)
    println("CUSTOM_LISTENER: numRecords = " + numRecords.toString)
    println("CUSTOM_LISTENER: processingDelay = " + processingDelay.toString)
    println("CUSTOM_LISTENER: schedulingDelay = " + schedulingDelay.toString)
    println("CUSTOM_LISTENER: totalDelay = " + totalDelay.toString)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    resetMetrics
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    if (appNotStarted) {
      initMetrics
      appNotStarted = false
    }
    sparkJobId = batchCompleted.batchInfo.batchTime.toString.replaceAll(" ms", "").toLong
    numRecords = batchCompleted.batchInfo.numRecords
    processingDelay = batchCompleted.batchInfo.processingDelay.get
    schedulingDelay = batchCompleted.batchInfo.schedulingDelay.get
    totalDelay = batchCompleted.batchInfo.totalDelay.get
    jobStartTime = batchCompleted.batchInfo.processingStartTime.get
    jobEndTime = batchCompleted.batchInfo.processingEndTime.get
    jobElapsedTimeInSecs = (jobEndTime - jobStartTime).toFloat / 1000.0f
    sparkJobResult = ""
    accumMetrics
    logJobMetrics
    printMetrics
  }

}