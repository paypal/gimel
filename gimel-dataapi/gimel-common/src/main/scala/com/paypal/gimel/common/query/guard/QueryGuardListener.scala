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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Instant}

import com.paypal.gimel.common.conf.{QueryGuardConfigs, QueryGuardConstants}
import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.logger.Logger

class QueryGuardListener[E >: QueryGuardDelayedEvent](spark: SparkSession,
                                                      discoveryType: String =
                                                        "job")
    extends SparkListener
    with Producer[E] {
  private val logger = new Logger(this.getClass.getName)
  private val stopped = new AtomicBoolean(true)
  private val HEADER: String = "[DISCOVERY] "
  private var _consumers: Seq[Consumer[E]] = Seq.empty

  override def onJobStart(jobStart: SparkListenerJobStart) {
    logger.info(
      s"${HEADER}Job[${jobStart.jobId}] started with ${jobStart.stageInfos.size} stages @ ${Instant.now()}"
    )
    if (!stopped.get) {
      val job = JobSubmitted(
        jobStart.jobId,
        discoveryType,
        System.currentTimeMillis(),
        jobTtl = GenericUtils.getValue(
          spark.conf,
          QueryGuardConfigs.JOB_TTL,
          QueryGuardConstants.DEFAULT_JOB_TTL
        ),
        delayTtl = GenericUtils.getValue(
          spark.conf,
          QueryGuardConfigs.DELAY_TTL,
          QueryGuardConstants.DEFAULT_DELAY_TTL
        )
      )
      logger.info(
        s"${HEADER}Proceeding to queue in Job[${jobStart.jobId}] onto QueryGuard"
      )
      publish(job)
    } else {
      logger.info(
        s"${HEADER}As QueryGuardListener is ${stopped.get()}," +
          s" unable to queue in Job[${jobStart.jobId}]"
      )
    }
  }

  override def publish(queryGuardEvent: E): Unit = {
    for (consumer <- _consumers) {
      consumer.consume(queryGuardEvent)
    }
  }

  override def onStageCompleted(
    stageCompleted: SparkListenerStageCompleted
  ): Unit = {
    logger.info(
      s"Stage ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks."
    )
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.info(
      s"Job[${jobEnd.jobId}] completed at ${new DateTime(jobEnd.time)}" +
        s" with result -> ${jobEnd.jobResult}"
    )
    super.onJobEnd(jobEnd)
  }

  override def registerConsumers(consumers: Seq[Consumer[E]]): Unit = {
    _consumers = consumers
  }

  def start(): Unit = {
    // toggle stopped to true
    stopped.set(false)
    logger.info(s"${HEADER}Started QueryGuardListener: $stopped")
  }

  def stop(): Unit = {
    // toggle stopped to true
    stopped.compareAndSet(false, true)
    logger.info(s"${HEADER}Stopped QueryGuardListener: $stopped")
  }
}
