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
import java.util.concurrent.{Delayed, TimeUnit}

import com.google.common.base.Objects
import com.google.common.primitives.Ints
import org.joda.time.DateTime

import com.paypal.gimel.logger.Logger

private[query] sealed trait QueryGuardEvent

private[query] trait QueryGuardDelayedEvent extends QueryGuardEvent with Delayed

private[query] case class JobSubmitted(jobId: Int,
                                       jobType: String,
                                       startTime: Long =
                                         Instant.now().toEpochMilli,
                                       estimatedJobEndTime: Long,
                                       estimatedDelayEndTime: Long)
    extends QueryGuardDelayedEvent {
  private val logger = Logger(this.getClass.getName)

  override def getDelay(unit: TimeUnit): Long = {
    val currentInstant = Instant.now().toEpochMilli
    val diff = estimatedDelayEndTime - currentInstant
    logger.info(
      s"[JobSubmitted] Comparing Job with ID: $jobId diff: $diff with end time:" +
        s" ${new DateTime(estimatedDelayEndTime)}, and current instant:" +
        s" ${new DateTime(currentInstant)}"
    )
    unit.convert(diff, TimeUnit.MILLISECONDS)
  }

  override def compareTo(o: Delayed): Int = {
    Ints.saturatedCast(
      this.estimatedDelayEndTime - o
        .asInstanceOf[JobSubmitted]
        .estimatedDelayEndTime
    )
  }

  override def toString: String =
    Objects
      .toStringHelper(this)
      .add("jobId", jobId)
      .add("jobType", jobType)
      .add("startTime", startTime)
      .add("estimatedJobEndTime", estimatedJobEndTime)
      .add("estimatedDelayEndTime", estimatedDelayEndTime)
      .toString
}

object JobSubmitted {
  def apply(jobId: Int,
            jobType: String,
            startTime: Long,
            jobTtl: Int,
            delayTtl: Int): JobSubmitted =
    new JobSubmitted(
      jobId,
      jobType,
      startTime,
      startTime + jobTtl,
      startTime + delayTtl
    )

  def apply(job: JobSubmitted, jobTtl: Int, delayTime: Long): JobSubmitted =
    new JobSubmitted(
      jobId = job.jobId,
      jobType = job.jobType,
      startTime = job.startTime,
      estimatedJobEndTime = job.startTime + jobTtl,
      estimatedDelayEndTime = delayTime
    )
}

private[query] case class JobKill(jobId: Int, jobType: String, reason: String)
    extends QueryGuardEvent {
  override def toString: String =
    Objects
      .toStringHelper(this)
      .add("jobId", jobId)
      .add("jobType", jobType)
      .add("reason", reason)
      .toString
}
