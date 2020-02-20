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

object SingletonConnection {

  private var connection: Connection = null

  def getConnection(url: String, username: String, password: String): Connection = synchronized {
    if (connection == null || connection.isClosed) {
      connection = DriverManager.getConnection(url, username, password)
    }
    connection
  }


  def getConnection(jdbcConnectionUtility: JDBCConnectionUtility): Connection = synchronized {
    if (connection == null || connection.isClosed) {
      connection = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
    }
    connection
  }
}
