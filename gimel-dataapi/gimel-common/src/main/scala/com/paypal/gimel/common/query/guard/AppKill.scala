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

package com.paypal.gimel.common.query.guard

import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.SparkSession

import com.paypal.gimel.logger.Logger

class AppKill[E](spark: SparkSession, incomingLogger: Option[Logger] = None)
    extends EventLoop[E](name = "app-kill-event-loop")
    with Consumer[E] {

  private val logger = Logger(this.getClass.getName)
  private val HEADER: String = "[APP-KILL] "

  override def consume(queryGuardEvent: E): Unit = {
    this.post(queryGuardEvent)
  }

  override protected def onReceive(event: E): Unit = {
    // kill the received job
    event match {
      case jobKill: JobKill if jobKill.jobType == "job" =>
        val jobInfo = spark.sparkContext.statusTracker.getJobInfo(jobKill.jobId)
        if (jobInfo.isDefined && JobExecutionStatus.RUNNING == jobInfo.get
              .status()) {
          logger.info(s"${HEADER}Proceeding to cancel Job: $jobKill")
          spark.sparkContext.cancelJob(jobKill.jobId, jobKill.reason)
        }
      case jobKill: JobKill if jobKill.jobType == "stage" =>
        val stageInfo =
          spark.sparkContext.statusTracker.getStageInfo(jobKill.jobId)
        if (stageInfo.isDefined && stageInfo.get.numActiveTasks() > 0) {
          logger.info(s"${HEADER}Proceeding to cancel Stage: $jobKill")
          spark.sparkContext.cancelStage(jobKill.jobId, jobKill.reason)
        }
    }
  }

  override protected def onError(e: Throwable): Unit = {
    // stop all the monitoring task
    logger.info(s"${HEADER}Proceeding to stop ${_name}")
    stop()
  }

}
