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

package com.paypal.gimel.common.storageadmin


import java.sql._

import com.paypal.gimel.logger.Logger

object JDBCAdminClient {
  val logger = Logger()

  /**
    * Create Teradata Table if it does not exists
    *
    * @param url           teradata data source URL
    * @param teradataTable table to be created
    */
  def createTeradataTableIfNotExists(url: String, username: String, password: String, teradataTable: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val createTable =
      s"""
         |CREATE  TABLE $teradataTable
         |(
         | id bigint,
         | "name" varchar(1000),
         | rev bigint
         |)
      """.stripMargin
    try {
      val teradataURL: String = s"$url/user=$username,password=$password"
      val con: Connection = DriverManager.getConnection(teradataURL)
      val stmt: Statement = con.createStatement()
      stmt.executeUpdate(createTable)
      logger.info("Table Created in Teradata:" + teradataTable)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Drop teradata table if it exists already
    *
    * @param url           teradata data source URL
    * @param teradataTable table to be dropped
    */
  def dropTeradataTableIfExists(url: String, username: String, password: String, teradataTable: String) {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val dropTable =
      s"""
         |DROP  TABLE $teradataTable
      """.stripMargin
    try {
      val teradataURL: String = s"$url/user=$username,password=$password"
      val con: Connection = DriverManager.getConnection(teradataURL)
      val stmt: Statement = con.createStatement()
      stmt.executeUpdate(dropTable)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    }
  }
}
