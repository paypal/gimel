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

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.RuntimeConfig

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.logger.Logger

object GenericUtils {

  /**
    * Utility to check if the incoming string has properly closed braces
    * @param s -> Incoming string to be validated
    * @return
    */
  def hasValidBraces(s: String): Boolean = {
    import util.control.Breaks._
    if (s == null || s.trim.isEmpty) {
      true
    } else {
      val stack = scala.collection.mutable.Stack[Char]()
      var isValid = true
      breakable {
        for (i <- 0 until s.length) {
          val curr = s.charAt(i)
          if (curr == '{' || curr == '(' || curr == '[') {
            stack.push(curr)
          }
          if (curr == '}' || curr == ')' || curr == ']') {
            if (stack.isEmpty) {
              isValid = false
              break
            }
            val peek = stack.top
            if (curr == '}' && peek == '{' || curr == ')' && peek == '(' || curr == ']' && peek == '[') {
              stack.pop
            } else {
              isValid = false
              break
            }
          }
        }
        if (isValid && stack.nonEmpty) {
          isValid = false
        }
      }
      isValid
    }
  }

  /**
    * Generic utility to parse long
    *
    * @param numeric -> Numeric string
    * @return
    */
  def parseLong(numeric: String): Option[Long] = {
    if (isNumeric(numeric)) {
      numeric match {
        case num if num.contains(GimelConstants.DOT) =>
          Try(num.substring(0, num.indexOf(GimelConstants.DOT)).toLong).toOption
        case num => Try(num.toLong).toOption
      }
    } else {
      None
    }
  }

  /**
    * Utility to check if the given string is a number, for double type string it removes the DOT and checks
    *
    * @param id
    * @return
    */
  def isNumeric(id: String): Boolean = {
    if (isStrNotEmpty(id)) {
      StringUtils.isNumeric(
        id.replace(GimelConstants.DOT, GimelConstants.EMPTY_STRING)
      )
    } else {
      false
    }
  }

  /**
    * Function to check if String is not Empty
    *
    * @param x -> incoming String
    * @return
    */
  def isStrNotEmpty(x: String): Boolean = x != null && x.trim.nonEmpty

  /**
    * Generic utility to parse int
    *
    * @param numeric -> Numeric string
    * @return
    */
  def parseInt(numeric: String): Option[Int] = {
    if (isNumeric(numeric)) {
      numeric match {
        case num if num.contains(GimelConstants.DOT) =>
          Try(num.substring(0, num.indexOf(GimelConstants.DOT)).toInt).toOption
        case num => Try(num.toInt).toOption
      }
    } else {
      None
    }
  }

  /**
    * Method to check if the fields are present in the Map or not.
    *
    * @param map    Map for which fields need to be validated
    * @param fields fields to be validated
    * @tparam k type of key for the Map
    * @tparam v type of value for the Map
    */
  def validateMapForFields[k, v](map: Map[k, v], fields: List[k]): Boolean = {
    for (fieldName <- fields) {
      if (!map.contains(fieldName)) {
        throw new IllegalArgumentException(
          s"Expected field: $fieldName not present."
        )
      }
    }
    true
  }

  /**
    * Function to determine the elapsed time taken for function execution
    *
    * @param block -> blocking call
    * @tparam R -> Result from blocking call
    * @return
    */
  def time[R](functionName: String = "Function Name not passed ",
              logger: Option[Logger] = None)(block: => R): R = {
    val timer = Timer()
    try {
      timer.start
      block // call-by-name
    } finally {
      val (_, timeTakenInWords) = timer.end
      val message =
        s"Time taken for function[FunctionName: $functionName] execution: $timeTakenInWords "
      if (logger.isDefined) {
        logger.get.info(message)
      } else {
        // scalastyle:off println
        println(message)
        // scalastyle:on println
      }
    }
  }

  /**
    *
    * @param text
    * @param searchString
    * @return
    */
  def containsWord(text: String, searchString: String): Boolean = {
    require(
      isStrNotEmpty(searchString),
      s"Expected search string[$searchString] to be available"
    )
    containsWord(text, s"\\b$searchString\\b".r)
  }

  /**
    *
    * @param text
    * @param regexPattern
    * @return
    */
  def containsWord(text: String, regexPattern: Regex): Boolean = {
    require(
      isStrNotEmpty(text),
      s"Expected incoming text[$text] to be available"
    )
    regexPattern.findFirstIn(text).isDefined
  }

  /**
    * Utitlity to create concurrent hashset
    * @tparam T
    * @return
    */
  def createSet[T](): mutable.Set[T] = {
    import scala.collection.JavaConverters._
    java.util.Collections
      .newSetFromMap(
        new java.util.concurrent.ConcurrentHashMap[T, java.lang.Boolean]
      )
      .asScala
  }

  def getValue[T](conf: SparkConf, key: String, defaultValue: T): T = {
    Try {
      val optionValue = if (conf.getOption(key).isDefined) {
        conf.getOption(key)
      } else {
        conf.getOption(s"spark.$key")
      }
      extractValue(defaultValue, optionValue)
    }.getOrElse(defaultValue)
  }

  private def extractValue[T](defaultValue: T, optionValue: Option[String]): T = {
    optionValue match {
      case Some(value) =>
        defaultValue match {
          case _: Int =>
            // Adding the fix for converting incoming seconds value to millis
            (value.toInt * 1000).asInstanceOf[T]
          case _: Long =>
            value.toLong.asInstanceOf[T]
          case _: Double =>
            value.toDouble.asInstanceOf[T]
          case _ => value.asInstanceOf[T]
        }
      case None => defaultValue.asInstanceOf[T]
    }
  }

  def getValue[T](conf: RuntimeConfig, key: String, defaultValue: T): T = {
    Try {
      val optionValue = if (conf.getOption(key).isDefined) {
        conf.getOption(key)
      } else {
        conf.getOption(s"spark.$key")
      }
      extractValue(defaultValue, optionValue)
    }.getOrElse(defaultValue)
  }

  def getValue[T](conf: Map[String, String], key: String, defaultValue: T): T = {
    Try {
      val optionValue = if (conf.get(key).isDefined) {
        conf.get(key)
      } else {
        conf.get(s"spark.$key")
      }
      extractValue(defaultValue, optionValue)
    }.getOrElse(defaultValue)
  }

  def getValueAny[T](conf: Map[String, Any], key: String, defaultValue: T): T = {
    Try {
      val optionValue = if (conf.get(key).isDefined) {
        conf.get(key)
      } else {
        conf.get(s"spark.$key")
      }
      extractValue(defaultValue, Some(optionValue.get.toString))
    }.getOrElse(defaultValue)
  }

  def getValueFailIfEmpty[T](conf: Map[String, String], key: String, msg: String): String = {
    val optionValue = conf.getOrElse(key, "").toString
    if (optionValue.isEmpty) {
      throw new IllegalArgumentException(msg)
    } else {
      optionValue
    }
  }

  def log(msg: String,
          classLogger: Logger,
          incomingLogger: Option[Logger] = None): Unit = {
    if (incomingLogger.isDefined) {
      incomingLogger.get.info(msg)
    } else {
      classLogger.info(msg)
    }
  }

  def logError(msg: String,
               exception: Throwable,
               classLogger: Logger,
               incomingLogger: Option[Logger] = None): Unit = {
    if (incomingLogger.isDefined) {
      incomingLogger.get.error(msg, exception)
    } else {
      classLogger.error(msg, exception)
    }
  }

  /**
    * Reads the config maps and if no matching key found, returns an empty string
    * @param key
    * @param userProps
    * @param systemProps
    * @return
    */
  def getConfigValue(key: String,
                     userProps: Map[String, String],
                     systemProps: Map[String, String],
                     defaultValue: String = GimelConstants.EMPTY_STRING): String = {
    userProps.getOrElse(
      s"spark.$key",
      systemProps.getOrElse(
        s"spark.$key",
        userProps.getOrElse(
          key,
          systemProps.getOrElse(key, defaultValue)
        )
      )
    )
  }

  /**
    * Reads the config maps and if no matching key found, returns an empty string
    *
    * @param key
    * @param userProps
    * @param systemProps
    * @return
    */
  def getConfigValueOption(key: String,
                           userProps: Map[String, String],
                           systemProps: Map[String, String]): Option[String] = {
    key match {
      case _ if userProps.contains(s"spark.$key") => userProps.get(s"spark.$key")
      case _ if userProps.contains(key) => userProps.get(key)
      case _ if systemProps.contains(s"spark.$key") => systemProps.get(s"spark.$key")
      case _ if systemProps.contains(key) => systemProps.get(key)
      case _ => None
    }
  }

  /**
    * Using try-with-resources, as described in https://medium.com/@dkomanov/scala-try-with-resources-735baad0fd7d
    *
    * @param r
    * @param f
    * @tparam T
    * @tparam V
    * @return
    */
  def withResources[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  private def closeAndAddSuppressed(e: Throwable,
                                    resource: AutoCloseable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }

  /**
    * Reads the config value matching to case in sensitive key
    *
    * @param key          -> incoming key to be looked upon from the props map
    * @param props        : Map[String, Any] -> Property map to be retrieved upon
    * @param defaultValue -> Default Value to be returned
    *
    * @return
    */
  def getConfigValueFromCaseInsensitiveKey(key: String,
                                           props: Map[String, Any],
                                           defaultValue: String = GimelConstants.EMPTY_STRING): String = {
    key match {
      case _ if props.contains(key) => props(key).toString
      case _ if props.contains(key.toLowerCase()) => props(key.toLowerCase()).toString
      case _ if props.contains(key.toUpperCase()) => props(key.toUpperCase()).toString
      case _ => defaultValue
    }
  }
}
