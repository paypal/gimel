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
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.json4s.DefaultFormats
import com.paypal.gimel.logging.Logging

/**
  * A {{SparkListener}} that captures and logs all metrics
  *
  * @param conf
  */
class GimelSparkListener(conf: SparkConf) extends SparkListener with Logging {

  def accumulateCrossJobs: Boolean = false

  private implicit def formats = DefaultFormats

  val DEFAULT_GROUP_ID: String = "DEFAULT_GROUP_ID"

  /**
    * Cluster and HostName
    */

  private val hadoopConfiguration = new org.apache.hadoop.conf.Configuration()
  val clusterUrl = hadoopConfiguration.get("fs.default.name")
  val clusterName = new java.net.URI(clusterUrl).getHost()
  val hostName = clusterName

  /**
    * Application Level Metrics
    */

  var applicationId: String = "Unknown"
  var appAttemptId: String = ""
  var appName: String = "Unknown"
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
    * Application or Job Level Metrics
    */
  var numberOfExecutors: Long = 0L
  var startNumberOfExecutors: Long = 0L
  var minNumberOfExecutors: Long = 0L
  var maxNumberOfExecutors: Long = 0L
  var endNumberOfExecutors: Long = 0L

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
  var appMetricExecutorRunTime = 0L
  var appMetricJvmGCTime = 0L
  var appMetricExecutorDeserializeTime = 0L
  var appInputRecordsRead = 0L
  var appInputBytesRead = 0L
  var appOutputBytesWritten = 0L
  var appOutputRecordsWritten = 0L
  var appShuffleRecordsRead = 0L
  var appShuffleRemoteBytesRead = 0L
  var appShuffleRecordsWritten = 0L
  var appShuffleBytesWritten = 0L
  var appShuffleWriteTime = 0L

  /**
    * Job Level Metrics
    */
  var metricExecutorRunTime: Long = 0L
  var metricJvmGCTime: Long = 0L
  var metricExecutorDeserializeTime: Long = 0L
  var metricResultSize: Long = 0L
  var metricResultSerializationTime: Long = 0L
  var metricMemoryBytesSpilled: Long = 0L
  var metricDiskBytesSpilled: Long = 0L
  var metricPeakExecutionMemory: Long = 0L
  var inputRecordsRead: Long = 0L
  var inputBytesRead: Long = 0L
  var outputBytesWritten: Long = 0L
  var outputRecordsWritten: Long = 0L
  var shuffleRecordsRead: Long = 0L
  var shuffleRemoteBytesRead: Long = 0L
  var shuffleRecordsWritten: Long = 0L
  var shuffleRemoteBlocksFetched: Long = 0L
  var shuffleLocalBlocksFetched: Long = 0L
  var shuffleFetchWaitTime: Long = 0L
  var shuffleLocalBytesRead: Long = 0L
  var shuffleBytesWritten: Long = 0L
  var shuffleWriteTime: Long = 0L

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
    if ((sparkLogLevel != "application") && (sparkLogLevel != "job") && (sparkLogLevel != "task")) {
      println("Invalid sparkLogLevel (" + sparkLogLevel + "). Valid options: [ application | job | task ]. So, setting sparkLogLevel to application.")
    }
  }

  /**
    * Accumulate Job Level Metrics after each job completion to compute Application Level Metrics.
    */
  def accumMetrics: Unit = {
    appMetricExecutorRunTime += metricExecutorRunTime
    appMetricJvmGCTime += metricJvmGCTime
    appMetricExecutorDeserializeTime += metricExecutorDeserializeTime
    appInputRecordsRead += inputRecordsRead
    appInputBytesRead += inputBytesRead
    appOutputBytesWritten += outputBytesWritten
    appOutputRecordsWritten += outputRecordsWritten
    appShuffleRecordsRead += shuffleRecordsRead
    appShuffleRemoteBytesRead += shuffleRemoteBytesRead
    appShuffleRecordsWritten += shuffleRecordsWritten
    appShuffleBytesWritten += shuffleBytesWritten
    appShuffleWriteTime += shuffleWriteTime
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

    startNumberOfExecutors = 0L
    endNumberOfExecutors = 0L
    metricExecutorRunTime = 0L
    metricJvmGCTime = 0L
    metricExecutorDeserializeTime = 0L
    inputRecordsRead = 0L
    inputBytesRead = 0L
    outputBytesWritten = 0L
    outputRecordsWritten = 0L
    shuffleRecordsRead = 0L
    shuffleRemoteBytesRead = 0L
    shuffleRecordsWritten = 0L
    shuffleBytesWritten = 0L
    shuffleWriteTime = 0L
  }

  /**
    * Log all the metrics both in JSON format and into Kafka
    *
    */
  def logAppMetrics: Unit = {
    kafkaLogger.info(this.appKafkaArgs)
  }

  def appKafkaArgs: JMap[String, Any] = {
    Map(

      // App Level Config Details
      "logtime" -> java.lang.System.currentTimeMillis(),
      "logType" -> "LivyMetrics",
      "sparkLogLevel" -> "Application",
      "host" -> hostName,
      "cluster" -> clusterName,
      "applicationId" -> applicationId,
      "appAttemptId" -> appAttemptId,
      "appName" -> appName,
      "sparkWebUI" -> sparkWebUI,
      "driverLogs" -> driverLogs,
      "sparkMaster" -> sparkMaster,
      "sparkUser" -> sparkUser,
      "sparkVersion" -> sparkVersion,
      "sparkEnvProperties" -> sparkEnvProperties,

      // App Level Metrics
      "appStartTime" -> appStartTime,
      "appEndTime" -> appEndTime,
      "appElapsedTimeInSecs" -> appElapsedTimeInSecs,
      "sparkDriverMemory" -> sparkDriverMemory,
      "sparkExecutorMemory" -> sparkExecutorMemory,
      "sparkExecutorCores" -> sparkExecutorCores,
      "sparkExecutorInstances" -> sparkExecutorInstances,
      "startNumberOfExecutors" -> startNumberOfExecutors,
      "endNumberOfExecutors" -> endNumberOfExecutors,
      "minNumberOfExecutors" -> minNumberOfExecutors,
      "maxNumberOfExecutors" -> maxNumberOfExecutors,

      // App or Job Level Metrics
      "appResult" -> sparkJobResult,
      "appErrorMessage" -> jobErrorMessage,
      "appErrorValue" -> jobErrorValue,
      "appErrorTrace" -> jobErrorTrace,
      "appSuccess" -> jobSuccess,
      "appFailure" -> jobFailure,

      // App or Job Level Metrics
      "metricExecutorRunTime" -> appMetricExecutorRunTime,
      "metricJvmGCTime" -> appMetricJvmGCTime,
      "metricExecutorDeserializeTime" -> appMetricExecutorDeserializeTime,
      "inputRecordsRead" -> appInputRecordsRead,
      "inputBytesRead" -> appInputBytesRead,
      "outputBytesWritten" -> appOutputBytesWritten,
      "outputRecordsWritten" -> appOutputRecordsWritten,
      "shuffleRecordsRead" -> appShuffleRecordsRead,
      "shuffleRemoteBytesRead" -> appShuffleRemoteBytesRead,
      "shuffleRecordsWritten" -> appShuffleRecordsWritten,
      "shuffleBytesWritten" -> appShuffleBytesWritten,
      "shuffleWriteTime" -> appShuffleWriteTime
    ).asJava
  }

  def logJobMetrics: Unit = {
    kafkaLogger.info(this.jobKafkaArgs)
  }

  def jobKafkaArgs: JMap[String, Any] = {
    Map(

      // App Level Config Details
      "logtime" -> java.lang.System.currentTimeMillis(),
      "logType" -> "GimelMetrics",
      "sparkLogLevel" -> "Job",
      "host" -> hostName,
      "cluster" -> clusterName,
      "applicationId" -> applicationId,
      "appAttemptId" -> appAttemptId,
      "appName" -> appName,

      // Job Level Config Details
      "sparkJobId" -> sparkJobId,
      "sparkJobResult" -> sparkJobResult,
      "jobSuccessStatus" -> jobSuccessStatus,
      "jobErrorMessage" -> jobErrorMessage,
      "jobErrorValue" -> jobErrorValue,
      "jobErrorTrace" -> jobErrorTrace,

      // Job Level Metrics
      "jobSuccess" -> jobSuccess,
      "jobFailure" -> jobFailure,
      "jobStartTime" -> jobStartTime,
      "jobEndTime" -> jobEndTime,
      "startNumberOfExecutors" -> startNumberOfExecutors,
      "endNumberOfExecutors" -> endNumberOfExecutors,

      // App or Job Level Metrics
      "metricExecutorRunTime" -> metricExecutorRunTime,
      "metricJvmGCTime" -> metricJvmGCTime,
      "metricExecutorDeserializeTime" -> metricExecutorDeserializeTime,
      "inputRecordsRead" -> inputRecordsRead,
      "inputBytesRead" -> inputBytesRead,
      "outputBytesWritten" -> outputBytesWritten,
      "outputRecordsWritten" -> outputRecordsWritten,
      "shuffleRecordsRead" -> shuffleRecordsRead,
      "shuffleRemoteBytesRead" -> shuffleRemoteBytesRead,
      "shuffleRecordsWritten" -> shuffleRecordsWritten,
      "shuffleBytesWritten" -> shuffleBytesWritten,
      "shuffleWriteTime" -> shuffleWriteTime
    ).asJava
  }

  def printMetrics: Unit = {
    println("CUSTOM_LISTENER: sparkVersion = " + sparkVersion)
    println("CUSTOM_LISTENER: logdate = " + date.toString)
    println("CUSTOM_LISTENER: logtime = " + timeStamp.toString)
    println("CUSTOM_LISTENER: host = " + hostName.toString)
    println("CUSTOM_LISTENER: cluster = " + clusterName.toString)
    println("CUSTOM_LISTENER: applicationId = " + applicationId.toString)
    println("CUSTOM_LISTENER: appName = " + appName.toString)
    println("CUSTOM_LISTENER: appAttemptId = " + appAttemptId.toString)
    println("CUSTOM_LISTENER: appStartTime = " + appStartTime.toString)
    println("CUSTOM_LISTENER: appEndTime = " + appEndTime.toString)
    println("CUSTOM_LISTENER: appElapsedTimeInSecs = " + appElapsedTimeInSecs.toString)
    println("CUSTOM_LISTENER: sparkMaster = " + sparkMaster.toString)
    println("CUSTOM_LISTENER: sparkUser = " + sparkUser.toString)
    println("CUSTOM_LISTENER: sparkEnvProperties = " + sparkEnvProperties.toString)
    println("CUSTOM_LISTENER: sparkLogLevel = " + sparkLogLevel.toString)
    println("CUSTOM_LISTENER: sparkDriverMemory = " + sparkDriverMemory.toString)
    println("CUSTOM_LISTENER: sparkExecutorMemory = " + sparkExecutorMemory.toString)
    println("CUSTOM_LISTENER: sparkExecutorCores = " + sparkExecutorCores.toString)
    println("CUSTOM_LISTENER: sparkExecutorInstances = " + sparkExecutorInstances.toString)
    println("CUSTOM_LISTENER: sparkWebUI = " + sparkWebUI.toString)
    println("CUSTOM_LISTENER: driverLogs = " + driverLogs.toString)
    println("CUSTOM_LISTENER: startNumberOfExecutors = " + startNumberOfExecutors.toString)
    println("CUSTOM_LISTENER: endNumberOfExecutors = " + endNumberOfExecutors.toString)
    println("CUSTOM_LISTENER: minNumberOfExecutors = " + minNumberOfExecutors.toString)
    println("CUSTOM_LISTENER: maxNumberOfExecutors = " + maxNumberOfExecutors.toString)
    println("CUSTOM_LISTENER: sparkJobId = " + sparkJobId.toString)
    println("CUSTOM_LISTENER: sparkJobResult = " + sparkJobResult.toString)
    println("CUSTOM_LISTENER: jobSuccess = " + jobSuccess.toString)
    println("CUSTOM_LISTENER: jobFailure = " + jobFailure.toString)
    println("CUSTOM_LISTENER: jobSuccessStatus = " + jobSuccessStatus)
    println("CUSTOM_LISTENER: jobErrorMessage = " + jobErrorMessage)
    println("CUSTOM_LISTENER: jobErrorValue = " + jobErrorValue)
    println("CUSTOM_LISTENER: jobErrorTrace = " + jobErrorTrace)
    println("CUSTOM_LISTENER: jobStartTime = " + jobStartTime.toString)
    println("CUSTOM_LISTENER: jobEndTime = " + jobEndTime.toString)
    println("CUSTOM_LISTENER: jobElapsedTimeInSecs = " + jobElapsedTimeInSecs.toString)
    println("CUSTOM_LISTENER: appMetricExecutorRunTime = " + appMetricExecutorRunTime.toString)
    println("CUSTOM_LISTENER: appMetricJvmGCTime = " + appMetricJvmGCTime.toString)
    println("CUSTOM_LISTENER: appMetricExecutorDeserializeTime = " + appMetricExecutorDeserializeTime.toString)
    println("CUSTOM_LISTENER: appInputRecordsRead = " + appInputRecordsRead.toString)
    println("CUSTOM_LISTENER: appInputBytesRead = " + appInputBytesRead.toString)
    println("CUSTOM_LISTENER: appOutputBytesWritten = " + appOutputBytesWritten.toString)
    println("CUSTOM_LISTENER: appOutputRecordsWritten = " + appOutputRecordsWritten.toString)
    println("CUSTOM_LISTENER: appShuffleRecordsRead = " + appShuffleRecordsRead.toString)
    println("CUSTOM_LISTENER: appShuffleRemoteBytesRead = " + appShuffleRemoteBytesRead.toString)
    println("CUSTOM_LISTENER: appShuffleRecordsWritten = " + appShuffleRecordsWritten.toString)
    println("CUSTOM_LISTENER: appShuffleBytesWritten = " + appShuffleBytesWritten.toString)
    println("CUSTOM_LISTENER: appShuffleWriteTime = " + appShuffleWriteTime.toString)
    println("CUSTOM_LISTENER: metricExecutorRunTime = " + metricExecutorRunTime.toString)
    println("CUSTOM_LISTENER: metricJvmGCTime = " + metricJvmGCTime.toString)
    println("CUSTOM_LISTENER: metricExecutorDeserializeTime = " + metricExecutorDeserializeTime.toString)
    println("CUSTOM_LISTENER: inputRecordsRead = " + inputRecordsRead.toString)
    println("CUSTOM_LISTENER: inputBytesRead = " + inputBytesRead.toString)
    println("CUSTOM_LISTENER: outputBytesWritten = " + outputBytesWritten.toString)
    println("CUSTOM_LISTENER: outputRecordsWritten = " + outputRecordsWritten.toString)
    println("CUSTOM_LISTENER: shuffleRecordsRead = " + shuffleRecordsRead.toString)
    println("CUSTOM_LISTENER: shuffleRemoteBytesRead = " + shuffleRemoteBytesRead.toString)
    println("CUSTOM_LISTENER: shuffleRecordsWritten = " + shuffleRecordsWritten.toString)
    println("CUSTOM_LISTENER: shuffleBytesWritten = " + shuffleBytesWritten.toString)
    println("CUSTOM_LISTENER: shuffleWriteTime = " + shuffleWriteTime.toString)
  }

  override def onApplicationStart(appStart: SparkListenerApplicationStart): Unit = {
    initMetrics
    applicationId = appStart.appId.get
    appName = appStart.appName
    appStartTime = appStart.time
    appStart.appAttemptId.foreach(appAttemptId = _)
    sparkUser = appStart.sparkUser
    appStart.driverLogs.foreach(logs => driverLogs = logs.toString)
  }

  override def onApplicationEnd(appEnd: SparkListenerApplicationEnd): Unit = {
    appEndTime = appEnd.time
    appElapsedTimeInSecs = (appEndTime - appStartTime).toFloat / 1000.0f
    if ((jobSuccess == 0) && (jobFailure == 0)) {
      jobSuccess = 1
      sparkJobResult = "JobSucceeded"
    }
    logAppMetrics
    printMetrics

  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    resetMetrics
    jobCompleted = false
    sparkJobId = jobStart.jobId
    jobStartTime = jobStart.time
    startNumberOfExecutors = numberOfExecutors
    if (sparkJobId == 0) {
      minNumberOfExecutors = numberOfExecutors
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobCompleted = true
    endNumberOfExecutors = numberOfExecutors
    jobEndTime = jobEnd.time
    jobElapsedTimeInSecs = (jobEndTime - jobStartTime).toFloat / 1000.0f
    val jobResult = jobEnd.jobResult
    sparkJobResult = jobResult.toString()
    if ((sparkJobResult == "JobSucceeded") ||
      (jobSuccessStatus.endsWith("livy.repl.Interpreter$ExecuteSuccess"))) {
      jobSuccess = 1
      jobFailure = 0
    } else {
      jobSuccess = 0
      jobFailure = 1
    }
    if (sparkLogLevel == "job") {
      logJobMetrics
      printMetrics
    }
    accumMetrics
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    numberOfExecutors += 1
    if (numberOfExecutors > maxNumberOfExecutors) {
      maxNumberOfExecutors = numberOfExecutors
    }
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    numberOfExecutors -= 1
    if (numberOfExecutors < minNumberOfExecutors) {
      minNumberOfExecutors = numberOfExecutors
    }
  }

  /**
    * Utility function to get required Task Metrics
    *
    * @param taskEnd SparkListenerTaskEnd
    * @return Task Metrics
    */
  def getTaskMetrics(taskEnd: SparkListenerTaskEnd): Map[String, Any] = {
    val metrics: TaskMetrics = taskEnd.taskMetrics
    Map(
      // App Level Config Details
      "logtime" -> java.lang.System.currentTimeMillis(),
      "logType" -> "GimelMetrics",
      "sparkLogLevel" -> "Task",
      "host" -> hostName,
      "cluster" -> clusterName,
      "applicationId" -> applicationId,
      "appAttemptId" -> appAttemptId,
      "appName" -> appName,
      // Task Level
      "taskId" -> taskEnd.taskInfo.taskId,
      "taskType" -> taskEnd.taskType,
      "stageId" -> taskEnd.stageId,
      "stageAttemptId" -> taskEnd.stageAttemptId,
      "gettingResultTime" -> taskEnd.taskInfo.gettingResultTime,
      "finishTime" -> taskEnd.taskInfo.finishTime,
      "executorId" -> taskEnd.taskInfo.executorId,
      "status" -> taskEnd.taskInfo.status,
      "attemptNumber" -> taskEnd.taskInfo.attemptNumber,
      "duration" -> taskEnd.taskInfo.duration,
      "isSpeculative" -> taskEnd.taskInfo.speculative,
      "launchTime" -> taskEnd.taskInfo.launchTime,
      "host" -> taskEnd.taskInfo.host
      , "metricExecutorRunTime" -> metrics.executorRunTime
      , "metricJvmGCTime" -> metrics.jvmGCTime
      , "metricExecutorDeserializeTime" -> metrics.executorDeserializeTime
      , "metricResultSize" -> metrics.resultSize
      , "metricResultSerializationTime" -> metrics.resultSerializationTime
      , "metricMemoryBytesSpilled" -> metrics.memoryBytesSpilled
      , "metricDiskBytesSpilled" -> metrics.diskBytesSpilled
      , "metricPeakExecutionMemory" -> metrics.peakExecutionMemory
      , "inputRecordsRead" -> metrics.inputMetrics.recordsRead
      , "inputBytesRead" -> metrics.inputMetrics.bytesRead
      , "outputRecordsWritten" -> metrics.outputMetrics.recordsWritten
      , "outputBytesWritten" -> metrics.outputMetrics.bytesWritten
      , "shuffleRecordsRead" -> metrics.shuffleReadMetrics.recordsRead
      , "shuffleRemoteBytesRead" -> metrics.shuffleReadMetrics.remoteBytesRead
      , "shuffleRemoteBlocksFetched" -> metrics.shuffleReadMetrics.remoteBlocksFetched
      , "shuffleLocalBlocksFetched" -> metrics.shuffleReadMetrics.localBlocksFetched
      , "shuffleFetchWaitTime" -> metrics.shuffleReadMetrics.fetchWaitTime
      , "shuffleLocalBytesRead" -> metrics.shuffleReadMetrics.localBytesRead
      , "shuffleRecordsWritten" -> metrics.shuffleWriteMetrics.recordsWritten
      , "shuffleBytesWritten" -> metrics.shuffleWriteMetrics.bytesWritten
      , "shuffleWriteTime" -> metrics.shuffleWriteMetrics.writeTime
    )
  }

  /**
    * Posts the Task Metrics to Sink System
    *
    * @param taskEnd SparkListenerTaskEnd
    */
  def logTaskMetrics(taskEnd: SparkListenerTaskEnd): Unit = {
    val metricsToPost = getTaskMetrics(taskEnd)
    kafkaLogger.info(metricsToPost.asJava)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    taskEnd.taskMetrics match {
      case null =>
        kafkaLogger.debug(s"Task metrics are not available for $taskEnd")
      case metrics =>
        logTaskMetrics(taskEnd)
        metricExecutorRunTime += metrics.executorRunTime
        metricJvmGCTime += metrics.jvmGCTime
        metricExecutorDeserializeTime += metrics.executorDeserializeTime
        metricResultSize += metrics.resultSize
        metricResultSerializationTime += metrics.resultSerializationTime
        metricMemoryBytesSpilled += metrics.memoryBytesSpilled
        metricDiskBytesSpilled += metrics.diskBytesSpilled
        metricPeakExecutionMemory += metrics.peakExecutionMemory

        inputRecordsRead += metrics.inputMetrics.recordsRead
        inputBytesRead += metrics.inputMetrics.bytesRead

        outputRecordsWritten += metrics.outputMetrics.recordsWritten
        outputBytesWritten += metrics.outputMetrics.bytesWritten

        shuffleRecordsRead += metrics.shuffleReadMetrics.recordsRead
        shuffleRemoteBytesRead += metrics.shuffleReadMetrics.remoteBytesRead
        shuffleRemoteBlocksFetched += metrics.shuffleReadMetrics.remoteBlocksFetched
        shuffleLocalBlocksFetched += metrics.shuffleReadMetrics.localBlocksFetched
        shuffleFetchWaitTime += metrics.shuffleReadMetrics.fetchWaitTime
        shuffleLocalBytesRead += metrics.shuffleReadMetrics.localBytesRead

        shuffleRecordsWritten += metrics.shuffleWriteMetrics.recordsWritten
        shuffleBytesWritten += metrics.shuffleWriteMetrics.bytesWritten
        shuffleWriteTime += metrics.shuffleWriteMetrics.writeTime
    }
  }
}