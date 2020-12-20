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

package com.paypal.gimel.hive.utilities

import java.security._
import java.sql.{Connection, DriverManager, Statement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import com.paypal.gimel.common.conf.{GimelConstants, GimelProperties}

object HiveJDBCUtils {

  def apply(conf: GimelProperties, cluster: String): HiveJDBCUtils = {
    new HiveJDBCUtils(conf, cluster)
  }
}

class HiveJDBCUtils(val props: GimelProperties, cluster: String = "unknown_cluster") {
  val logger = com.paypal.gimel.logger.Logger()

  logger.info("Using Supplied KeyTab to authenticate KDC...")
  val conf = new Configuration
  conf.set(GimelConstants.SECURITY_AUTH, "kerberos")
  UserGroupInformation.setConfiguration(conf)
  val ugi: UserGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(props.principal, props.keytab)
  UserGroupInformation.setLoginUser(ugi)


  /**
    * Gives a HIVE JDBC connection to use.
    * Connection creation and closing will be taken care.
    *
    * @param fn to be executed with connection.
    * @return
    */
  def withConnection(fn: Connection => Any): Any = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.info("Trying to create hive connection")
    val connection = ugi.doAs(new PrivilegedAction[Connection] {
      override def run(): Connection = DriverManager.getConnection(props.hiveURL(cluster))
    }
    )
    var output: Any = None
    try {
      output = fn(connection)
    } catch {
      case e: Throwable =>
        e.printStackTrace
        throw e
    } finally {
      if (!connection.isClosed) {
        connection.close
      }
    }
    output
  }

  /**
    * Gives a HIVE JDBC statement to use.
    * Statement creation and closing will be taken care along with connection.
    *
    * @param fn to be executed with connection.
    * @return
    */
  def withStatement(fn: Statement => Any): Any = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    withConnection {
      connection =>
        val statement = connection.createStatement
        var output: Any = None
        try {
          output = fn(statement)
        } catch {
          case e: Throwable =>
            e.printStackTrace
            throw e
        }
        finally {
          if (!statement.isClosed) {
            statement.close
          }
        }
        output
    }
  }

}

