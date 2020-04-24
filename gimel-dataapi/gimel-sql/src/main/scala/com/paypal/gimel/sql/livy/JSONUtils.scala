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

package com.paypal.gimel.sql.livy

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.ShortTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read

object JSONUtils {
  implicit val formats = Serialization.formats(
    ShortTypeHints(
      List()
    )
  )
  /**
    * convert the JSON into state object
    * @param message - incoming json string
    * @tparam T - incoming scala class which we want to convert the json string to scala object
    * @return - the supplied scala class object
    */

  def convertToJson[T: Manifest](message: String): Option[T] = {
    try {
      Some(read[T](message))
    } catch {
      case a: com.fasterxml.jackson.core.JsonParseException =>
        println(message)
        None
      case b: org.json4s.MappingException =>
        println("Mapping exception")
        None
    }
  }

  /**
    * print the json object
    * @param message
    */

  def printToJson(message: Object): Unit = {
    val mapper: ObjectMapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    println(mapper.writerWithDefaultPrettyPrinter.writeValueAsString(message))
  }


  /**
    * Prints the incoming JSON string
    * @param data
    */

  def printToJson(data: String): Unit = {
    if (data == null) {
      return
    }
    val mapper: ObjectMapper = new ObjectMapper()
    try {
      val obj = mapper.readValue(data, classOf[Object])
      println(mapper.writerWithDefaultPrettyPrinter.writeValueAsString(obj))
    } catch {
      case a: JsonParseException =>
        println(data)
    }
  }

}
