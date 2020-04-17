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
import java.util.Properties

import org.apache.spark.sql.SparkSession

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.exception.JDBCConnectionError
import com.paypal.gimel.jdbc.utilities.JdbcAuxiliaryUtilities.getJDBCSystem
import com.paypal.gimel.logger.Logger

/**
  * This object defines the connection object required any time during Sparksession
  */
object JDBCConnectionUtility {
  def apply(sparkSession: SparkSession, dataSetProps: Map[String, Any],
            connectionDetails: Option[PartitionUtils.ConnectionDetails] = None): JDBCConnectionUtility =
    new JDBCConnectionUtility(sparkSession: SparkSession, dataSetProps: Map[String, Any], connectionDetails)

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
    GenericUtils.withResources(r)(f)
  }

  def getJdbcPasswordStrategy(dataSetProperties: Map[String, Any]): String = {
    dataSetProperties.getOrElse(JdbcConfigs.jdbcPasswordStrategy,
      JdbcConstants.JDBC_DEFAULT_PASSWORD_STRATEGY).toString
  }

}

class JDBCConnectionUtility(sparkSession: SparkSession,
                            dataSetProps: Map[String, Any],
                            connectionDetails: Option[PartitionUtils.ConnectionDetails] = None) extends Serializable {
  private val authUtilities: JDBCAuthUtilities = JDBCAuthUtilities(sparkSession)

  // get url
  val url: String = JdbcAuxiliaryUtilities.getJdbcUrl(dataSetProps, connectionDetails)

  // get JDBC system type
  val jdbcSystem: String = getJDBCSystem(url)

  val jdbcPasswordStrategy: String = JDBCConnectionUtility.getJdbcPasswordStrategy(dataSetProps)

  val gts_default_user = sparkSession.conf.get(GimelConstants.GTS_DEFAULT_USER_FLAG, "")

  // get actual JDBC user
  // Negating with file strategy, as in many places other than "file" default case is handled!
  var jdbcUser: String = if (sparkSession.sparkContext.sparkUser.equalsIgnoreCase(gts_default_user)
    && !JdbcConstants.JDBC_FILE_PASSWORD_STRATEGY.equals(jdbcPasswordStrategy)) {
    // Validate incoming user to be able to set query band
    val gtsUser: String = sparkSession.sparkContext.getLocalProperty(GimelConstants.GTS_USER_CONFIG)
    if (Option(gtsUser).isEmpty){
      throw new IllegalStateException("Expecting local user to be available for establishing JDBC connection")
    }
    gtsUser
  } else {
    JDBCCommons.getJdbcUser(dataSetProps, sparkSession)
  }

  // Get username and password
  private val (userName, password) = authUtilities.getJDBCCredentials(url, dataSetProps)

  // check valid access
  //  val validUserAccess: Boolean = verifyUserForQueryBand()

  /**
    * This method returns the connection object
    *
    * @return
    */
  def getJdbcConnection(logger: Option[Logger] = None): Connection = {
    try {
      jdbcSystem match {
        case JdbcConstants.TERADATA =>
          // For making sure unit and integration tests are able to access
          Class.forName("com.teradata.jdbc.TeraDriver")
          if(logger.isDefined) {
            logger.get.info(s"In Teradata Get JDBC connection, with dataset properties: $dataSetProps")
          }
        case JdbcConstants.MYSQL =>
          // For making sure unit and integration tests are able to access
          Class.forName("com.mysql.jdbc.Driver")
          if(logger.isDefined) {
            logger.get.info(s"In MYSQL Get JDBC connection, with dataset properties: $dataSetProps")
          }
        case _ => // Do nothing
      }
      if (logger.isDefined) {
        logger.get.info(s"Creating connection with URL: $url")
      }
      DriverManager.getConnection(url, userName, password)
    }
    catch {
      case e: Throwable =>
        throw new JDBCConnectionError(s"Error getting JDBC connection: ${e.getMessage}", e)
    }
  }

  /**
    * This method sets the queryband and returns the connection object
    *
    * @return connection
    */
  def getJdbcConnectionAndSetQueryBand(logger: Option[Logger] = None): Connection = {
    val con = getJdbcConnection(logger)
    jdbcSystem match {
      case JdbcConstants.TERADATA =>
        JDBCCommons.setQueryBand(con, url, jdbcUser, jdbcPasswordStrategy)
      case _ =>
      // do nothing
    }
    con
  }


  /**
    * This is PayPal specific verification in order to restrict anyone to override user via proxyuser
    *
    */
  def verifyUserForQueryBand(user: String): Unit = {
    val sparkUser = sparkSession.sparkContext.sparkUser
    jdbcSystem match {
      case JdbcConstants.TERADATA =>
        if (jdbcPasswordStrategy.equalsIgnoreCase(JdbcConstants.JDBC_DEFAULT_PASSWORD_STRATEGY)) {
          // check For GTS queries
          // If sparkUser = livy AND GTS user != jdbcUser, then throw exception.
          if (sparkUser.equalsIgnoreCase(gts_default_user)) {
            // GTS Block
            val gtsUser: String = sparkSession.sparkContext.getLocalProperty(GimelConstants.GTS_USER_CONFIG)
            if (!user.equalsIgnoreCase(gtsUser)) {
              throw new IllegalAccessException(
                s"""SECURITY VIOLATION | [${gtsUser}] attempting Teradata Query Band Via JDBC User [${user}]
                   |This Operation is NOT permitted."""
                  .stripMargin
              )
            }
          }
          // check for non-GTS queries
          // If sparkUser != livy AND jdbcUser != sparkUser, then throw exception.
          if (!sparkUser.equalsIgnoreCase(GimelConstants.GTS_DEFAULT_USER) && !user.equalsIgnoreCase(sparkUser)) {
            throw new IllegalAccessException(
              s"""
                 |SECURITY VIOLATION | [${sparkUser}] attempting Teradata Query Band Via JDBC User [${user}]
                 |This Operation is NOT permitted."""
                .stripMargin
            )
          }
        }
      case _ =>
      // do nothing
    }
  }

  /**
    *
    * @return
    */
  def getConnectionProperties: Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${userName}")
    connectionProperties.put("password", s"${password}")

    // set driver class
    val driverClass = JdbcAuxiliaryUtilities.getJdbcStorageOptions(dataSetProps)(JdbcConfigs.jdbcDriverClassKey)
    connectionProperties.setProperty("Driver", driverClass)

    connectionProperties
  }

}
