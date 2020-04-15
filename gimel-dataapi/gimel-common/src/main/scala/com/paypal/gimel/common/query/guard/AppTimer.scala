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

import java.time.Instant

import scala.collection.mutable

import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.SparkSession

import com.paypal.gimel.common.conf.{QueryGuardConfigs, QueryGuardConstants}
import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.logger.Logger

class AppTimer[T](spark: SparkSession, incomingLogger: Option[Logger] = None)
    extends DelayedEventLoop(name = "app-timer-event-loop")
    with Producer[T]
    with Consumer[QueryGuardDelayedEvent]
    with StatusTracker[QueryGuardDelayedEvent] {

  private val logger = Logger(this.getClass.getName)
  private val _submittedJobs: mutable.Set[Int] = GenericUtils.createSet[Int]()
  private val HEADER: String = "[APP-TIMER] "
  private val UNKNOWN_JOB_STATUS: String = "UNKNOWN_JOB_STATUS"
  private var _consumers: Seq[Consumer[T]] = Seq.empty

  /**
    * Checks the availability of incoming event on the queue
    * @param event -> Event to be checked for availability in queue
    * @return
    */
  override def contains(event: QueryGuardDelayedEvent): Boolean = {
    event match {
      case job: JobSubmitted =>
        if (!_submittedJobs.contains(job.jobId)) {
          println(job)
        }
        _submittedJobs.contains(job.jobId)
      case _ => false
    }
  }

  override def registerConsumers(consumers: Seq[Consumer[T]]): Unit = {
    _consumers = consumers
  }

  override def consume(queryGuardEvent: QueryGuardDelayedEvent): Unit = {
    queryGuardEvent match {
      case job: JobSubmitted =>
        _submittedJobs.add(job.jobId)
        this.post(queryGuardEvent)
    }

  }

  override protected def onReceive(event: QueryGuardDelayedEvent): Unit = {
    // once this app crosses the timeline kill the application, by publishing it to app kill
    event match {
      case job: JobSubmitted =>
        logger.info(s"${HEADER}Received job: $job")
        val jobInfo = spark.sparkContext.statusTracker.getJobInfo(job.jobId)
        if (jobInfo.isDefined && JobExecutionStatus.RUNNING == jobInfo.get
              .status()) {
          val currentJobTTL = GenericUtils.getValue(
            spark.conf,
            QueryGuardConfigs.JOB_TTL,
            QueryGuardConstants.DEFAULT_JOB_TTL
          )
          val jobEndTime: Long = job.startTime + currentJobTTL
          val currentInstant = Instant.now().toEpochMilli
          logger.info(
            s"${HEADER}For job[id: ${job.jobId}] checking end time: ${Instant.ofEpochMilli(jobEndTime)} " +
              s"against the current time: ${Instant.ofEpochMilli(currentInstant)}"
          )
          if (currentInstant < jobEndTime) {
            val reConfiguredJob = JobSubmitted(
              job,
              currentJobTTL,
              currentInstant + GenericUtils.getValue(
                spark.conf,
                QueryGuardConfigs.DELAY_TTL,
                QueryGuardConstants.DEFAULT_DELAY_TTL
              )
            )
            logger.info(
              s"${HEADER}Re-Queuing the job[id: ${reConfiguredJob.jobId}] as $reConfiguredJob"
            )
            this.post(reConfiguredJob)
          } else {
            val jobKill = JobKill(
              job.jobId,
              job.jobType,
              QueryGuardConstants.EXCEPTION_MSG_FORMAT.format(DurationFormatUtils
                .formatDurationWords(currentJobTTL, true, true),
                QueryGuardConstants.LOCAL_DATETIME_FORMATTER.print(job.startTime),
                QueryGuardConstants.LOCAL_DATETIME_FORMATTER.print(System.currentTimeMillis()))
            ).asInstanceOf[T]
            logger.info(
              s"${HEADER}Pushing the job: $jobKill onto job kill queue"
            )
            _submittedJobs.remove(job.jobId)
            publish(jobKill)
          }
        } else {
          logger.info(
            s"${HEADER}Job[${job.jobId} already completed with status: ${if (jobInfo.isDefined) {
              jobInfo.get.status()
            } else { UNKNOWN_JOB_STATUS }}]"
          )
        }
    }
  }

  override def publish(queryGuardEvent: T): Unit = {
    for (consumer <- _consumers) {
      consumer.consume(queryGuardEvent)
    }
  }

  override protected def onError(e: Throwable): Unit = {
    // stop all the monitoring task
    logger.info(
      s"${HEADER}OnError: clearing all the submitted jobs[id's: ${_submittedJobs}]"
    )
    _submittedJobs.clear()
    stop()
  }
}

object AppTimer {
  type QueueType = QueryGuardDelayedEvent
}
