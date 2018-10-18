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

/**
  *  This object defines the connection object required any time during Sparksession
  */
object JDBCConnectionUtility {
  def apply(sparkSession: SparkSession, dataSetProps: Map[String, Any]): JDBCConnectionUtility = new JDBCConnectionUtility(sparkSession: SparkSession, dataSetProps: Map[String, Any])
}

class JDBCConnectionUtility (sparkSession: SparkSession, dataSetProps: Map[String, Any]) extends Serializable {
  private val authUtilities: JDBCAuthUtilities = JDBCAuthUtilities(sparkSession)

  // get url
  val url = JdbcAuxiliaryUtilities.getJdbcUrl(dataSetProps)

  // get actual user
  val realUser: String = JDBCCommons.getDefaultUser(sparkSession)

  // Get username and password
  private val (userName, password) = authUtilities.getJDBCCredentials(url, dataSetProps)

  val jdbcPasswordStrategy = dataSetProps.getOrElse(JdbcConfigs.jdbcPasswordStrategy, JdbcConstants.jdbcDefaultPasswordStrategy).toString


  /**
    * This method returns the connection object
    * @return
    */
  def getJdbcConnection(): Connection = {
    DriverManager.getConnection(url, userName, password)
  }

  /**
    * This method sets the queryband and returns the connection object
    * @return connection
    */
  def getJdbcConnectionAndSetQueryBand(): Connection = {
    val con = getJdbcConnection()
    JDBCCommons.setQueryBand(con, url, realUser, jdbcPasswordStrategy)
    con
  }


}
