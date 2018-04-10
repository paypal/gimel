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

import com.paypal.gimel.common.storageadmin.HDFSAdminClient
import com.paypal.gimel.logger.Logger


object JDBCCommons extends Logger {

  /**
    * This method returns the JDBC password from the given password file
    *
    * @param jdbcURL  JDBBC URL to match
    * @param userName JDBC username
    * @return password
    */
  def getJdbcP(passwordFile: String, principal: String, keyTabPath: String, jdbcURL: String, userName: String): String = {

    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    info(" @Begin --> " + MethodName)

    // val fileContent = HDFSAdminClient.standaloneHDFSRead(passwordFile, principal, keyTabPath).toString
    val fileContent = scala.io.Source.fromFile(passwordFile).getLines().mkString("\n")
    val lines: Array[String] = fileContent.split("\n")
    info("Lines => " + lines)
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
      error(errorMsg)
      throw new JDBCAuthException(errorMsg)
    }
    password
  }


  /**
    * This method returns the Hive metastore password from the given password file
    *
    * @param passwordFile  password file path
    * @param storageSystemName storage system name
    * @return password
    */
  def getHivePassword(passwordFile: String, storageSystemName: String): String = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    info(" @Begin --> " + MethodName)
    val fileContent = scala.io.Source.fromFile(passwordFile).getLines().mkString("\n")
    val lines: Array[String] = fileContent.split("\n")
    info("Lines => " + lines)
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
      error(errorMsg)
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

}
