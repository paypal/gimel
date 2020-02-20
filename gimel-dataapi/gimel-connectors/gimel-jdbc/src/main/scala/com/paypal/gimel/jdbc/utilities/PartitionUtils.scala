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

import java.sql.Connection

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.SQLDataTypesUtils
import com.paypal.gimel.jdbc.conf.JdbcConfigs

object PartitionUtils {

  val MOD_COLUMN_NAME: String = "MOD_COL"

  /**
    * Given a table, datastore type, custom partition definitions returns the applicable partition columns
    *
    * @param conf       -> Configuration parameters
    * @param connection -> java.sql.Connection
    * @return
    */
  def getPartitionColumns(conf: Map[String, String],
                          connection: Connection,
                          numericOnlyFlag: Boolean = false): Seq[String] = {
    conf.get(JdbcConfigs.jdbcPartitionColumns) match {
      case Some(partitionColumn) =>
        val partitionCols =
          partitionColumn.split(GimelConstants.COMMA).toSeq
        // TODO validate if the partition columns are available on the table else throw Runtime exception
        // TODO if numeric only flag triggered then check whether the primary key columns are numeric and available
        partitionCols
      case None
        if conf.contains(JdbcConfigs.jdbcUrl) && conf.contains(
          JdbcConfigs.jdbcDbTable
        ) => JdbcAuxiliaryUtilities.getPrimaryKeys(conf(JdbcConfigs.jdbcUrl), conf(JdbcConfigs.jdbcDbTable),
        connection, numericOnlyFlag)
      case None => Seq.empty
    }
  }

  /**
    * Given a Partition sequence generated in accordance with the user configured partition columns or
    * table index columns, is merged with the incoming sql
    *
    * @param incomingSql -> SQL received from driver
    * @param partitionColumns -> User configured partition columns or table index columns
    * @param numOfPartitions -> User configured number of partitions or table partitions
    * @param currentPartition -> Current partition index
    * @return -> Merged SQL with partition sequence
    */
  def getMergedPartitionSequence(incomingSql: String,
                                 partitionColumns: Seq[String],
                                 numOfPartitions: Int,
                                 currentPartition: Int = -1): String = {
    SQLDataTypesUtils.mergeConditionClause(
      incomingSql,
      generateTeradataPartitionSequence(
        partitionColumns,
        numOfPartitions,
        currentPartition
      )
    )
  }

  /**
    *
    * @param partitionColumns -> Incoming partition columns
    * @return
    */
  def generateTeradataPartitionSequence(partitionColumns: Seq[String],
                                        numOfPartitions: Int,
                                        currentPartition: Int): String =
    s"HASHBUCKET (HASHROW (${partitionColumns.mkString(",")})) MOD $numOfPartitions " +
      s"= ${if (currentPartition < 0) "?" else currentPartition}"

  /**
    * A wrapper to hold all the partition specific information
    *
    * @param partitionColumns  A Sequence of columns to be used for partitioning the dataset
    * @param lowerBound    the minimum value of the first placeholder
    * @param upperBound    the maximum value of the second placeholder
    *                      The lower and upper bounds are inclusive.
    * @param numOfPartitions the number of partitions.
    *                      Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
    *                      the query would be executed twice, once with (1, 10) and once with (11, 20)
    *
    */
  case class PartitionInfoWrapper(jdbcSystem: String,
                                  partitionColumns: Seq[String],
                                  lowerBound: Long,
                                  upperBound: Long,
                                  numOfPartitions: Int)

  case class ConnectionDetails(numOfPartitions: Int,
                               fetchSize: Int,
                               jdbcUrlType: String,
                               errLimit: Option[Int] = None) {
    override def toString: String =
      s"[partitions: $numOfPartitions, fetchSize: $fetchSize," +
        s" connectionType: $jdbcUrlType] & errLimit: $errLimit"
  }
  object ConnectionDetails {
    val JDBC_CONNECTION_URL_TYPE: String = "JDBC"
    val FASTEXPORT_CONNECTION_URL_TYPE: String = "FASTEXPORT"
    val FASTLOAD_CONNECTION_URL_TYPE: String = "FASTLOAD"
  }
}
