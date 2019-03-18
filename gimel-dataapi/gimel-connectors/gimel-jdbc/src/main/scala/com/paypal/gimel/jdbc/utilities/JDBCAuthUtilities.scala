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

package com.paypal.gimel.jdbc.utilities

import org.apache.spark.sql.SparkSession

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.storageadmin._
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.logger.Logger

object JDBCAuthUtilities {
  def apply(sparkSession: SparkSession): JDBCAuthUtilities = new JDBCAuthUtilities(sparkSession: SparkSession)
}

/**
  * Utilities to resolve the username and password specific to a JDBC data source.
  */
class JDBCAuthUtilities(sparkSession: SparkSession) extends Serializable {

  val logger = Logger(this.getClass)

  /**
    * Returns the spark user.
    * Get USERNAME and PASSWORD from HiveContext conf.
    * USERNAME can be user specified using this configuration parameter:
    * {{{
    * "spark.jdbc.username=USERNAME"
    * }}}
    * If not specified, it will be taken from sparkUser argument.
    *
    * @param dataSetProps dataset properties
    * @return username of the user executing the job
    */
  private def getUserName(dataSetProps: Map[String, Any]): String = {
    val defaultValue = getHiveConf(JdbcConstants.jdbcUserName, sparkSession.sparkContext.sparkUser)
    dataSetProps.getOrElse(JdbcConstants.jdbcUserName, defaultValue).toString
  }

  private def getHiveConf(key: String, default: String) = {
    sparkSession.sparkContext.getConf.get(key, default)
  }

  /**
    * This function returns system user
    *
    * @return sys env user
    */
  private def getSystemUser(): String = {
    sys.env(GimelConstants.USER)
  }

  /**
    * This function returns appropriate password file e.g individual user/batch user
    *
    * @param dataSetProps dataset properties
    * @return passwordFILE
    *
    */
  private def getPasswordFile(dataSetProps: Map[String, Any]): String = {
    val userName = getUserName(dataSetProps)
    val pFile = JdbcConstants.pFile.replace(GimelConstants.USER_NAME, userName)
    val defaultValue = getHiveConf(JdbcConfigs.jdbcP, pFile)
    dataSetProps.getOrElse(JdbcConfigs.jdbcP, defaultValue).toString
  }

  /**
    * This function returns the Gimel JDBC password
    *
    * @param dataSetProps dataset properties
    * @return Gimel JDBC password
    */
  private def getInlinePassword(dataSetProps: Map[String, Any]): String = {
    dataSetProps.getOrElse(JdbcConfigs.jdbcPassword, new String()).toString
  }

  /**
    * Returns the username and password corresponding to current user.
    *
    * @param url The URL for the JDBC data source.
    * @return a Tuple of (userName, password) used for accessing JDBC data source.
    */
  private def getDBCredentials(url: String, dataSetProps: Map[String, Any]): (String, String) = {
    val jdbcPasswordStrategy =
      dataSetProps.getOrElse(JdbcConfigs.jdbcPasswordStrategy, JdbcConstants.jdbcDefaultPasswordStrategy).toString
    val userName = getUserName(dataSetProps)
    var password: String = ""
    jdbcPasswordStrategy match {
      case "file" => {
        val passwordFile = getPasswordFile(dataSetProps)
        val fileContent = HDFSAdminClient.readHDFSFile(passwordFile)
        val lines: Array[String] = fileContent.split("\n")
        var flag: Boolean = false
        lines.foreach { line =>
          // get the url for data source in the password file to verify with the actual table url
          val userURL = line.split(",")(0)
          val dataSourceURL = userURL.split("/")(0)
          // get the actual user in the password file to verify with the actual spark user
          val urlLength = userURL.split("/").length
          val actualUser = userURL.split("/")(urlLength - 1)
          password = line.split(",")(1)
          // Verify the URL and Username in passwordFile with spark user
          if (url.contains(dataSourceURL) && actualUser.equalsIgnoreCase(userName)) {
            flag = true
            return (userName, password)
          }
        }
        if (!flag) {
          val msg =
            """Username or Password NOT FOUND!
              |Check the configuration parameter or password file"""".stripMargin
          throw new JDBCAuthException(msg, null)
        }
        (userName, password)
      }
      case "inline" => {
        logger.warn("gimel.jdbc.p.strategy=inline is NOT a secure way to supply password. " +
          "Please switch to gimel.jdbc.p.strategy=file")
        val password = getInlinePassword(dataSetProps)
        if(password.length() == 0) {
          throw new JDBCAuthException("gimel.jdbc.p.strategy=inline was supplied, " +
            "but gimel.jdbc.password is NOT set", null)
        }
        return (userName, password)
      }
      case _ => {
        val msg = """Invalid jdbcPasswordStrategy"""".stripMargin
        throw new JDBCAuthException(msg, null)
      }
    }
  }

  /**
    * This function the returns username and password corresponding to current user.
    *
    * @param url url is the URL for the JDBC data source
    * @return a Tuple of (userName, password) used for accessing JDBC data source
    */
  def getJDBCCredentials(url: String, dataSetProps: Map[String, Any]): (String, String) = {
    val (userName, password) = getDBCredentials(url, dataSetProps)
    (userName, password)
  }
}

/**
  * Custom Exception for JDBCDataSet initiation errors.
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
class JDBCAuthException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}
