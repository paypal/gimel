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

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession

import com.paypal.gimel.common.conf.{QueryGuardConfigs, QueryGuardConstants}
import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.common.utilities.GenericUtils._
import com.paypal.gimel.logger.Logger

class AppDiscovery[E >: QueryGuardDelayedEvent](name: String = "app-discovery",
                                                spark: SparkSession,
                                                statusTracker: StatusTracker[E],
                                                incomingLogger: Option[Logger] =
                                                  None,
                                                discoveryType: String = "job")
    extends Producer[E] {
  private val stopped = new AtomicBoolean(true)
  private val logger = new Logger(this.getClass.getName)
  private val HEADER: String = "DISCOVERY "
  private var _consumers: Seq[Consumer[E]] = Seq.empty

  def start(): Unit = {
    // toggle stopped to false
    if (stopped.compareAndSet(true, false)) {
      // Initiating the discovery process again
      init().start()
    }
  }

  private def init(): Thread = {
    // A timed loop discovery process start, until stopped -> false
    // Start a thread and set it to daemon, poll spark until the apps are found and post to the event loop
    new Thread(name) {
      setDaemon(true)

      override def run(): Unit = {
        try {
          while (!stopped.get) {
            try {
              onReceive()
            } catch {
              case NonFatal(e) =>
                try {
                  onError(e)
                } catch {
                  case NonFatal(e) =>
                    logError(
                      s"${HEADER}Unexpected error in $name",
                      e,
                      logger,
                      incomingLogger
                    )
                }
            }
          }
        } catch {
          case ie: InterruptedException => // exit even if eventQueue is not empty
          case NonFatal(e) =>
            logError(
              s"${HEADER}Unexpected error in $name",
              e,
              logger,
              incomingLogger
            )
        }
      }

    }
  }

  def onReceive(): Unit = {
    // perform the app discovery and post to event loop
    discoveryType match {
      case "job" =>
        publishInfo(spark.sparkContext.statusTracker.getActiveJobIds())
      case "stage" =>
        publishInfo(spark.sparkContext.statusTracker.getActiveStageIds())
    }

  }

  private def publishInfo(ids: Array[Int]): Unit = {
    for {
      activeJobId <- ids
      job = JobSubmitted(
        activeJobId,
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
      if !statusTracker.contains(event = job)
    } {
      publish(job)
    }
  }

  override def publish(queryGuardEvent: E): Unit = {
    for (consumer <- _consumers) {
      consumer.consume(queryGuardEvent)
    }
  }

  def onError(e: Throwable): Unit = {
    // stop all the monitoring task
    stop()
  }

  def stop(): Unit = {
    // toggle stopped to true
    log(s"${HEADER}Proceeding to stop discovery", logger, incomingLogger)
    stopped.compareAndSet(false, true)
  }

  override def registerConsumers(consumers: Seq[Consumer[E]]): Unit = {
    _consumers = consumers
  }
}
