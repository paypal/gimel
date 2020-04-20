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
import com.paypal.gimel.common.security.FileHandler
import com.paypal.gimel.common.storageadmin._
import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.exception._
import com.paypal.gimel.logger.Logger

object JDBCAuthUtilities {
  def apply(sparkSession: SparkSession): JDBCAuthUtilities = new JDBCAuthUtilities(sparkSession: SparkSession)
}

/**
  * Utilities to resolve the username and password specific to a JDBC data source.
  */
class JDBCAuthUtilities(sparkSession: SparkSession) extends Serializable {

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
    val logger = Logger(this.getClass.getName)
    val userName = JDBCCommons.getJdbcUser(dataSetProps, sparkSession)
    val pFile = JdbcConstants.P_FILEPATH.replace(GimelConstants.USER_NAME, userName)
    val defaultValue = sparkSession.conf.get(JdbcConfigs.jdbcP, pFile)
    if (dataSetProps.get(JdbcConfigs.jdbcP) == None) {
      logger.info(
        s"""
           | Please specify configuration ${JdbcConfigs.jdbcP} with local (along with option ${JdbcConfigs.jdbcPFileSource} as ${JdbcConstants.LOCAL_FILE_SOURCE}) or HDFS file location containing password.
           | Otherwise password file will be searched in default location ${defaultValue}. Please check docs for more info.
         """.stripMargin)
    }
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
    val logger = Logger(this.getClass.getName)
    var password: String = ""
    var userName = JDBCCommons.getJdbcUser(dataSetProps, sparkSession)

    // get password strategy for JDBC
    val jdbcPasswordStrategy = dataSetProps.getOrElse(JdbcConfigs.jdbcPasswordStrategy, JdbcConstants.JDBC_DEFAULT_PASSWORD_STRATEGY).toString

    jdbcPasswordStrategy match {
      case JdbcConstants.JDBC_FILE_PASSWORD_STRATEGY => {
        val passwordFile = getPasswordFile(dataSetProps)
        logger.info(s"Password file provided by user: ${passwordFile}")

        val fileSource = dataSetProps.getOrElse(JdbcConfigs.jdbcPFileSource, JdbcConstants.DEFAULT_P_FILE_SOURCE).toString
        fileSource match {

          case JdbcConstants.LOCAL_FILE_SOURCE => {
            password = JDBCCommons.getJdbcP(passwordFile, "", "", url, userName)
          }
          case _ => {
            FileHandler.warnIfFileAccessibleByOthers(passwordFile, GimelConstants.HADDOP_FILE_SYSTEM)
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
                s"""Username or Password NOT FOUND!\n
                   | Check the password file: ${passwordFile} or configuration parameters provided in API
                   | Please specify password in file as: jdbc_url/username,PASSWORD.
              """.stripMargin
              throw new JdbcAuthenticationException(msg)
            }
          }
        }
      }
      case JdbcConstants.JDBC_INLINE_PASSWORD_STRATEGY => {
        logger.warning("gimel.jdbc.p.strategy=inline is NOT a secure way to supply password. " +
          "Please switch to gimel.jdbc.p.strategy=file")
        password = getInlinePassword(dataSetProps)
        if (password.length() == 0) {
          throw new JDBCAuthException("gimel.jdbc.p.strategy=inline was supplied, " +
            "but gimel.jdbc.password is NOT set", null)
        }
      }
      case JdbcConstants.JDBC_CUSTOM_PASSWORD_STRATEGY => {
        userName = JdbcConstants.JDBC_PROXY_USERNAME
        // Loading the custom auth provider at runtime
        val authLoaderClassName = GenericUtils.getValueAny(dataSetProps, JdbcConfigs.jdbcAuthLoaderClass, "")
        if (authLoaderClassName.isEmpty) {
          throw new IllegalArgumentException(s"You need to set the property ${JdbcConfigs.jdbcAuthLoaderClass} " +
            s"with ${JdbcConfigs.jdbcPasswordStrategy} = ${JdbcConstants.JDBC_CUSTOM_PASSWORD_STRATEGY}")
        }
        val authLoader = Class.forName(authLoaderClassName).newInstance.asInstanceOf[com.paypal.gimel.common.security.AuthProvider]
        password = authLoader.getCredentials(dataSetProps ++ Map(GimelConstants.GIMEL_AUTH_REQUEST_TYPE -> JdbcConstants.JDBC_AUTH_REQUEST_TYPE))
      }
      case _ => {
        val msg = """Invalid jdbcPasswordStrategy"""".stripMargin
        throw new JDBCAuthException(msg, null)
      }
    }
    (userName, password)
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
