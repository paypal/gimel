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

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
  * This is an extension from org.apache.spark.util.EventLoop, with support for having a delayed queue
  */
private[guard] abstract class EventLoop[E](name: String) extends Logging {

  private val stopped = new AtomicBoolean(false)
  private val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
      try {
        while (!stopped.get) {
          val event = eventQueue.take()
          try {
            onReceive(event)
          } catch {
            case NonFatal(e) =>
              try {
                onError(e)
              } catch {
                case NonFatal(e) => logError("Unexpected error in " + name, e)
              }
          }
        }
      } catch {
        case ie: InterruptedException => // exit even if eventQueue is not empty
        case NonFatal(e) =>
          logError("Unexpected error in " + name, e)
      }
    }

  }

  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    onStart()
    eventThread.start()
  }

  /**
    * Invoked when `start()` is called but before the event thread starts.
    */
  protected def onStart(): Unit = {}

  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      eventThread.interrupt()
      var onStopCalled = false
      try {
        eventThread.join()
        // Call onStop after the event thread exits to make sure onReceive happens before onStop
        onStopCalled = true
        onStop()
      } catch {
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          if (!onStopCalled) {
            // ie is thrown from `eventThread.join()`. Otherwise, we should not call `onStop` since
            // it's already called.
            onStop()
          }
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
    }
  }

  /**
    * Invoked when `stop()` is called and the event thread exits.
    */
  protected def onStop(): Unit = {}

  /**
    * Put the event into the event queue. The event thread will process it later.
    */
  def post(event: E): Unit = {
    eventQueue.put(event)
  }

  protected val eventQueue: BlockingQueue[E] = new LinkedBlockingQueue[E]()

  /**
    * Return if the event thread has already been started but not yet stopped.
    */
  def isActive: Boolean = eventThread.isAlive

  /**
    * Invoked in the event thread when polling events from the event queue.
    *
    * Note: Should avoid calling blocking actions in `onReceive`, or the event thread will be blocked
    * and cannot process events in time. If you want to call some blocking actions, run them in
    * another thread.
    */
  protected def onReceive(event: E): Unit

  /**
    * Invoked if `onReceive` throws any non fatal error. Any non fatal error thrown from `onError`
    * will be ignored.
    */
  protected def onError(e: Throwable): Unit

  private[gimel] def _name: String = this.name
}

private[query] trait Producer[E] {
  def registerConsumers(consumers: Seq[Consumer[E]]): Unit
  def publish(e: E): Unit
}
private[query] trait Consumer[E] {
  def consume(e: E): Unit
}
private[query] trait StatusTracker[E] {

  /**
    * Checks the availability of incoming event on the queue
    * @param event -> QueryGuardEvent to be checked for availability in queue
    * @return
    */
  def contains(event: E): Boolean = false
}
