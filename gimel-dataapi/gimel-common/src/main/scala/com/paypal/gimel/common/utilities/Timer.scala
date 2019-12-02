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

package com.paypal.gimel.common.utilities

import java.time.Instant

import org.apache.commons.lang3.time.DurationFormatUtils

/**
  * Timer object to capture start and end of module.
  */

object Timer {
  def apply(): Timer = {
    val k: Timer = new Timer()
    k
  }
}

class Timer() {
  private lazy val (endTime1, totalTimeMillSec) = {
    val endTime1 = Some(Instant.now().toEpochMilli)
    (endTime1, (endTime1.get - startTime.get).toDouble)
  }
  //  private var endTime: Option[Long] = None
  private var startTime: Option[Long] = None

  def start: Option[Long] = {
    startTime = Some(Instant.now().toEpochMilli)
    startTime
  }

  def endWithMillSecRunTime: Double = totalTimeMillSec

  def end: (Long, String) = {
    val timeTaken = endTime.get - startTime.get
    (timeTaken, DurationFormatUtils.formatDurationWords(timeTaken, true, true))
  }

  def endTime: Some[Long] = endTime1
}
