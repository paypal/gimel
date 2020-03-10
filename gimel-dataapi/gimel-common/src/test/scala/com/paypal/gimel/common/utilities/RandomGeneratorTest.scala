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

import org.scalatest._

class RandomGeneratorTest extends FunSpec with Matchers {
  describe ("getRandomInt") {
    it ("should return a random integer in the range specified") {
      val random = RandomGenerator.getRandomInt(10, 20)
      assert(random >= 10 && random < 20)

      val random2 = RandomGenerator.getRandomInt(10, 100)
      assert(random2 >= 10 && random2 < 100)
    }
  }

  describe ("getRandomString") {
    it ("should return a string of random length") {
      assert(RandomGenerator.getRandomString(10).length() == 10)
    }
  }
}
