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

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.SparkSession

import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.utilities.JdbcAuxiliaryUtilities._
import com.paypal.gimel.logger.Logger


object JDBCCommons {

  val logger = Logger(this.getClass)

  /**
    * This method returns the JDBC password from the given password file
    *
    * @param jdbcURL  JDBBC URL to match
    * @param userName JDBC username
    * @return password
    */
  def getJdbcP(passwordFile: String, principal: String, keyTabPath: String, jdbcURL: String, userName: String): String = {

    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()

    logger.info(" @Begin --> " + MethodName)

    val fileContent = scala.io.Source.fromFile(passwordFile).getLines().mkString("\n")
    val lines: Array[String] = fileContent.split("\n")
    var password = ""
    var validURI: Boolean = false

    lines.foreach { line =>
      if (!line.equals("")) {
        // get the url for data source in the password file to verify with the actual table url
        val userURL = line.split(",")(0)
        val dataSourceURL = userURL.split("/")(0)
        // get the actual user in the password file to verify with the actual spark user
        val urlLength = userURL.split("/").length
        val actualUser = userURL.split("/")(urlLength - 1)
        // Verify the URL and Username in passwordFile with spark user
        if (jdbcURL.contains(dataSourceURL) && actualUser.equalsIgnoreCase(userName)) {
          validURI = true
          password = line.split(",")(1)
        }
      }
    }
    if (!validURI) {
      val errorMsg = "Username or Password NOT FOUND!!\nCheck the configuration parameter or password file"
      logger.error(errorMsg)
      throw new JDBCAuthException(errorMsg)
    }
    password
  }


  /**
    * This method returns the Hive metastore password from the given password file
    *
    * @param passwordFile      password file path
    * @param storageSystemName storage system name
    * @return password
    */
  def getHivePassword(passwordFile: String, storageSystemName: String): String = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()

    logger.info(" @Begin --> " + MethodName)
    val fileContent = scala.io.Source.fromFile(passwordFile).getLines().mkString("\n")
    val lines: Array[String] = fileContent.split("\n")
    var password = ""
    var validURI: Boolean = false

    lines.foreach { line =>
      if (!line.equals("")) {
        val dataSourceWithUser = line.split(",")(0)
        val dataSource = dataSourceWithUser.split("/")(0)
        if (storageSystemName.contains(dataSource)) {
          validURI = true
          password = line.split(",")(1)
        }
      }
    }
    if (!validURI) {
      val errorMsg = "Username or Password NOT FOUND!!\nCheck the configuration parameter or password file"
      logger.error(errorMsg)
      throw new JDBCAuthException(errorMsg)
    }
    password
  }


  /**
    *
    * @param url
    * @param userName
    * @param password
    * @return
    */
  def getJdbcConnection(url: String, userName: String, password: String): Connection = {
    DriverManager.getConnection(url, userName, password)
  }

  /**
    * get JDBC User
    *
    * @param dataSetProps
    * @param sparkSession
    * @return
    */
  def getJdbcUser(dataSetProps: Map[String, Any], sparkSession: SparkSession): String = {
    // get real user of JDBC
    val defaultValue = sparkSession.conf.get(JdbcConfigs.jdbcUserName, sparkSession.sparkContext.sparkUser)
    val username = dataSetProps.getOrElse(JdbcConfigs.jdbcUserName, defaultValue).toString
    username
  }


  /**
    * This method sets Query Band for Teradata
    *
    * @param conn
    * @param actualUser
    */
  def setQueryBand(conn: Connection, url: String, actualUser: String,
                   jdbcPasswordStrategy: String = JdbcConstants.JDBC_DEFAULT_PASSWORD_STRATEGY): Unit = {

    val jdbcSystem = getJDBCSystem(url)

    jdbcSystem match {

      case JdbcConstants.TERADATA =>
        val queryBandStatement: String = s"""SET QUERY_BAND = 'PROXYUSER=$actualUser;' FOR SESSION;"""
        jdbcPasswordStrategy match {
          case "file" => // do nothing
          case _ =>
            logger.info(s"Setting QueryBand for $actualUser")
            try {
              executeQueryStatement(queryBandStatement, conn)
            }
            catch {
              case ex: Throwable =>
                logger.error(s"Setting QueryBand failed for --> $actualUser")
                ex.printStackTrace()
                throw ex
            }
        }

      case _ =>
      // do nothing

    }
  }

  /**
    * This method resets all the configs required to be reset after action in JDBC spark Read/Write API
    *
    * @param sparkSession
    */
  def resetPushDownConfigs(sparkSession: SparkSession): Unit = {

    // reset JDBC Read type to Batch
    logger.info(s"Resetting ${JdbcConfigs.teradataReadType} to ${JdbcConstants.DEFAULT_READ_TYPE}")
    sparkSession.conf.set(JdbcConfigs.teradataReadType, JdbcConstants.DEFAULT_READ_TYPE)

    // reset JDBC Write type to Batch
    logger.info(s"Resetting ${JdbcConfigs.teradataWriteType} to ${JdbcConstants.DEFAULT_WRITE_TYPE}")
    sparkSession.conf.set(JdbcConfigs.teradataWriteType, JdbcConstants.DEFAULT_WRITE_TYPE)

    // reset JDBC default write strategy to insert
    logger.info(s"Resetting ${JdbcConfigs.jdbcInsertStrategy} to ${JdbcConstants.DEFAULT_INSERT_STRATEGY}")
    sparkSession.conf.set(JdbcConfigs.jdbcInsertStrategy, JdbcConstants.DEFAULT_INSERT_STRATEGY)

    // reset JDBC pushdownflag
    //    logger.info(s"Resetting ${JdbcConfigs.jdbcPushDownEnabled} to False")
    //    sparkSession.conf.set(JdbcConfigs.jdbcPushDownEnabled, "false")

    // unsetting the jdbc url from spark conf
    logger.info(s"Unsetting ${JdbcConfigs.jdbcUrl} from spark conf")
    sparkSession.conf.unset(JdbcConfigs.jdbcUrl)

    // unsetting the jdbc username from spark conf
    // logger.info(s"Unsetting ${JdbcConfigs.jdbcUserName} from spark conf")
    // sparkSession.conf.unset(JdbcConfigs.jdbcUserName)

    // unsetting the jdbc driver class from spark conf
    logger.info(s"Unsetting ${JdbcConfigs.jdbcDriverClassKey} from spark conf")
    sparkSession.conf.unset(JdbcConfigs.jdbcDriverClassKey)

    // unsetting jdbc table name from spark conf
    logger.info(s"Unsetting ${JdbcConfigs.jdbcInputTableNameKey} from spark conf")
    sparkSession.conf.unset(JdbcConfigs.jdbcInputTableNameKey)

  }

  /**
    * This method resets all the configs required to be reset after action in JDBC spark Write API
    *
    * @param sparkSession
    */
  def resetWriteConfigs(sparkSession: SparkSession): Unit = {

    // reset JDBC Write type to Batch
    logger.info(s"Resetting ${JdbcConfigs.teradataWriteType} to ${JdbcConstants.DEFAULT_WRITE_TYPE}")
    sparkSession.conf.set(JdbcConfigs.teradataWriteType, JdbcConstants.DEFAULT_WRITE_TYPE)

    // reset JDBC default write strategy to insert
    logger.info(s"Resetting ${JdbcConfigs.jdbcInsertStrategy} to ${JdbcConstants.DEFAULT_INSERT_STRATEGY}")
    sparkSession.conf.set(JdbcConfigs.jdbcInsertStrategy, JdbcConstants.DEFAULT_INSERT_STRATEGY)

  }

  /**
    * This method resets all the configs required to be reset after action in JDBC spark Read API
    *
    * @param sparkSession
    */
  def resetReadConfigs(sparkSession: SparkSession): Unit = {

    // reset JDBC Read type to Batch
    logger.info(s"Resetting ${JdbcConfigs.teradataReadType} to ${JdbcConstants.DEFAULT_READ_TYPE}")
    sparkSession.conf.set(JdbcConfigs.teradataReadType, JdbcConstants.DEFAULT_READ_TYPE)
  }


}
