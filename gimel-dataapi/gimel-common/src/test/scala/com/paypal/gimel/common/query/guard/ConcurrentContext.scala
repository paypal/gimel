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

import org.apache.spark.sql.SparkSession

import com.paypal.gimel.logger.Logger

object ConcurrentContext {
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent._
  import scala.concurrent.duration.Duration
  import scala.concurrent.duration.Duration._

  private val logger: Logger = Logger(this.getClass.getName)

  /**
    * Awaits only a set of elements at a time. At most batchSize futures will ever
    * be in memory at a time
    */
  def awaitBatch[T](it: Iterator[Future[T]],
                    batchSize: Int = 3,
                    timeout: Duration = Inf): Iterator[T] = {
    it.grouped(batchSize)
      .map(batch => Future.sequence(batch))
      .flatMap(futureBatch => Await.result(futureBatch, timeout))
  }

  def performAction(spark: SparkSession,
                    sleepTimeMs: Long = 5000,
                    size: Int = 10): Unit = {
    logger.info(
      s"Submitting spark task with sleepTimeMs: $sleepTimeMs and size: $size"
    )
    spark.sparkContext
      .parallelize(1 to size)
//      .map(fastFoo)
      .map(x => ConcurrentContext.executeAsync(slowFoo(x), sleepTimeMs))
      .mapPartitions(it => ConcurrentContext.awaitAll(it))
      .foreach(x => logger.info(s"Finishing with $x"))
  }

  /*
   * Awaits an entire sequence of futures and returns an iterator. This will
    wait for all futures to complete before returning
   */
  def awaitAll[T](it: Iterator[Future[T]],
                  timeout: Duration = Inf): Iterator[T] = {
    Await.result(Future.sequence(it), timeout)
  }

  /** Wraps a code block in a Future and returns the future */
  def executeAsync[T](f: => T): Future[T] = {
    Future(f)
  }

  /*
   * Waits five second then returns the input. Represents some kind of IO operation
   */
  def slowFoo[T](x: T, sleeptTimeMs: Long = 5000): T = {
    logger.info(s"slowFoo start ($x)")
    Thread.sleep(sleeptTimeMs)
    logger.info(s"slowFoo end ($x)")
    x
  }

  def startSparkjob(spark: SparkSession,
                    sleepTimeMs: Long = 5000,
                    size: Int = 10): Unit = {
    logger.info(
      s"Submitting spark task with sleepTimeMs: $sleepTimeMs and size: $size"
    )
    spark.sparkContext
      .parallelize(1 to size)
      .map(x => fastFoo(x, sleepTimeMs))
//      .map(x => ConcurrentContext.executeAsync(slowFoo(x), sleepTimeMs))
//      .mapPartitions(it => ConcurrentContext.awaitAll(it))
      .foreach(x => logger.info(s"Finishing with $x"))
  }

  /**
    * Immediately returns the input
    * @param x
    * @tparam T
    * @return
    */
  def fastFoo[T](x: T, sleeptTimeMs: Long = 5000): T = {
    logger.info(s"fastFoo start ($x)")
    Thread.sleep(sleeptTimeMs)
    logger.info(s"fastFoo end ($x)")
    x
  }
}
