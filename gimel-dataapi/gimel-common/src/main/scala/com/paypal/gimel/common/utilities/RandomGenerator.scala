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

import scala.util.Random

object RandomGenerator {

  /**
    * Returns a random integer between a given range
    *
    * @param low Range minimum
    * @param high Range maximum
    * @return Random integer between range [low, high)
    */
  def getRandomInt(low: Int, high: Int): Int = Random.nextInt(high - low) + low

  /**
    * Returns a random string of given length
    *
    * @param length:  Length of the random string to be generated
    * @return Random string of specified length
    */
  def getRandomString(length: Int): String = (Random.alphanumeric take length).mkString("")

}
