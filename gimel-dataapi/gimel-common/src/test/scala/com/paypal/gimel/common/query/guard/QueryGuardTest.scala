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

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Try}

import org.scalatest.FunSuite

import com.paypal.gimel.common.conf.QueryGuardConfigs
import com.paypal.gimel.common.utilities.spark.SharedSparkSession
import com.paypal.gimel.logger.Logger

class QueryGuardTest extends FunSuite with SharedSparkSession {
  override protected val additionalConfig: Map[String, String] = Map(
    QueryGuardConfigs.JOB_TTL -> "60000"
  )

  import ConcurrentContext._
  private val logger = Logger()

  def startAppAsync(jobSleepTimeoutConfig: Map[Int, Long] = Map.empty,
                    eachRunLength: Int = 10): Unit = {
    val scheduledJobs: immutable.Seq[Future[Unit]] =
      for (jobId <- 0 until jobSleepTimeoutConfig.size) yield {
        executeAsync(
          performAction(spark, jobSleepTimeoutConfig(jobId), eachRunLength)
        )
      }
    awaitAll(scheduledJobs.toIterator)
  }

  def startAppSync(jobSleepTimeoutConfig: Map[Int, Long] = Map.empty,
                   eachRunLength: Int = 10): Unit = {
    for (jobId <- 0 until jobSleepTimeoutConfig.size) {
      startSparkjob(spark, jobSleepTimeoutConfig(jobId), eachRunLength)
    }
  }

  test(
    "Query guard eviction with all the tasks completing within the scheduled time interval"
  ) {
    spark.conf.set(QueryGuardConfigs.DELAY_TTL, "1000")
    val jobSleepTimeoutConfig: Map[Int, Long] =
      Map(0 -> 5000, 1 -> 4000, 2 -> 500, 3 -> 2500)
    logger.setLogLevel("CONSOLE")
    val queryGuard: QueryGuard = new QueryGuard(spark)
    queryGuard.start()
    startAppAsync(jobSleepTimeoutConfig)
    queryGuard.stop()
  }

  test("Query guard eviction with synchronous timed task execution") {
    spark.conf.set(QueryGuardConfigs.JOB_TTL, "3000")
    spark.conf.set(QueryGuardConfigs.DELAY_TTL, "1000")
    val jobSleepTimeoutConfig: Map[Int, Long] =
      Map(0 -> 500, 1 -> 2500, 2 -> 1500, 3 -> 2800)
    logger.setLogLevel("CONSOLE")
    val queryGuard: QueryGuard = new QueryGuard(spark)
    queryGuard.start()
    startAppSync(jobSleepTimeoutConfig, 1)
    queryGuard.stop()
  }

  ignore("Ignoring this test") {
    test("Query guard eviction with app fail criteria") {
      spark.conf.set(QueryGuardConfigs.JOB_TTL, "3000")
      spark.conf.set(QueryGuardConfigs.DELAY_TTL, "1000")
      val jobSleepTimeoutConfig: Map[Int, Long] =
        Map(0 -> 500, 1 -> 2500, 2 -> 1500, 3 -> 4000)
      logger.setLogLevel("CONSOLE")
      val queryGuard: QueryGuard = new QueryGuard(spark)
      queryGuard.start()
      Try {
        startAppSync(jobSleepTimeoutConfig, 1)
      } match {
        case Failure(exception) =>
          logger.error(exception.getMessage)
          assert(
            exception.getMessage
              .contains(
                "cancelled as it reached the max TTL: 3 seconds, with Job start time "
              )
          )
        case _ =>
          throw new AssertionError("Expected an exception wiht TTL breach")
      }
      queryGuard.stop()
    }
  }

  test("looping") {
    for {
      cntr <- 1 until 23
      hr = "%02d".format(cntr)
    } {
      println(s" query where hr =$hr")
    }
  }
}
