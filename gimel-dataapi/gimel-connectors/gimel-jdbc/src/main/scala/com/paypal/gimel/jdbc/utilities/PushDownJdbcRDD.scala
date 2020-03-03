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

import java.sql.{Connection, ResultSet}

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.Row

import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.logger.Logger

/**
  * Push down JDBC queries always run as a single task, hence not overriding the partitions
  *
  * @param sc
  * @param getConnection
  * @param sql
  * @param mapRow
  */
class PushDownJdbcRDD(sc: SparkContext,
                      getConnection: () => Connection,
                      sql: String,
                      mapRow: ResultSet => Row = PushDownJdbcRDD.resultSetToRow)
  extends JdbcRDD[Row](sc, getConnection, sql, 0, 100, 1, mapRow)
    with Logging {

  override def compute(thePart: Partition,
                       context: TaskContext): Iterator[Row] = {
    val logger = Logger(this.getClass.getName)
    val functionName = s"[QueryHash: ${sql.hashCode}]"
    logger.info(s"Proceeding to execute push down query $functionName: $sql")
    val queryResult: String = GenericUtils.time(functionName, Some(logger)) {
      JDBCConnectionUtility.withResources(getConnection()) { connection =>
        JdbcAuxiliaryUtilities.executeQueryAndReturnResultString(
          sql,
          connection
        )
      }
    }
    Seq(Row(queryResult)).iterator
  }
}

object PushDownJdbcRDD {
  def resultSetToRow(rs: ResultSet): Row = {
    Row(rs.getString(0))
  }
}
