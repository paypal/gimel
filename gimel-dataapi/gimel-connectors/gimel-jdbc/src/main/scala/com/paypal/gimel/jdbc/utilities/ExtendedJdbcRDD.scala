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

import java.sql.{Connection, PreparedStatement, ResultSet}

import scala.reflect.ClassTag

import com.teradata.jdbc.jdk6.JDK6_SQL_Clob
import org.apache.commons.io.IOUtils
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.JdbcRDD

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.jdbc.conf.JdbcConstants
import com.paypal.gimel.jdbc.utilities.PartitionUtils._
import com.paypal.gimel.logger.Logger


// TODO: Expose a ExtendedJdbcRDD function in SparkContext and mark this as semi-private
/**
  * An RDD that executes a SQL query on a JDBC connection and reads results.
  * For usage example, see test case JdbcRDDSuite.
  *
  * ExtendedJdbcRDD implementation extends the default JdbcRDD class from open source spark implementation
  * url - https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/rdd/JdbcRDD.scala).
  * This implementation is use case specific implementation in order to execute the user specified statements
  * [In this specific case, setting queryband for Teradatan database.] This way user can specify any statements
  * that are to be executed before required sql query runs for READ/WRITE data.
  *
  *
  * @param getConnection a function that returns an open Connection.
  *                      The RDD takes care of closing the connection.
  * @param sql           the text of the query.
  *                      The query must contain two ? placeholders for parameters used to partition the results.
  *                      For example,
  *                      {{{
  *                         select title, author from books where ? <= id and id <= ?
  *                      }}}
  * @param mapRow        a function from a ResultSet to a single row of the desired result type(s).
  *                      This should only call getInt, getString, etc; the RDD takes care of calling next.
  *                      The default maps a ResultSet to an array of Object.
  */
class ExtendedJdbcRDD[T: ClassTag](
                                    sc: SparkContext,
                                    getConnection: () => Connection,
                                    sql: String,
                                    fetchSize: Int,
                                    partitionInfoWrapper: PartitionInfoWrapper,
                                    mapRow: ResultSet => T = ExtendedJdbcRDD.resultSetToObjectArray _
                                  )
  extends JdbcRDD[T](sc, getConnection, sql, partitionInfoWrapper.lowerBound,
    partitionInfoWrapper.upperBound, partitionInfoWrapper.numOfPartitions, mapRow) with Logging {

  override def getPartitions: Array[Partition] = {
    partitionInfoWrapper.jdbcSystem match {
      case JdbcConstants.TERADATA =>
        (0 until partitionInfoWrapper.numOfPartitions).map { i =>
          TeradataJdbcPartition(i, partitionInfoWrapper.partitionColumns, partitionInfoWrapper.numOfPartitions)
        }.toArray
      case _ =>
        val logger = Logger(this.getClass.getName)
        val length = BigInt(1) + partitionInfoWrapper.upperBound - partitionInfoWrapper.lowerBound
        (0 until partitionInfoWrapper.numOfPartitions).map { i =>
          val start = partitionInfoWrapper.lowerBound + ((i * length) / partitionInfoWrapper.numOfPartitions)
          val end = partitionInfoWrapper.lowerBound + (((i + 1) * length) / partitionInfoWrapper.numOfPartitions) - 1
          logger.info(s"Bounds for partition=$i --> lowerBound=$start,upperBound=$end ")
          new ModifiedJdbcPartition(i, start.toLong, end.toLong)
        }.toArray
    }
  }


  override def compute(incomingPartition: Partition, context: TaskContext): Iterator[T] = new Iterator[T] {

    private var gotNext = false
    private var nextValue: T = _
    private var closed = false
    private var recordsRead: Int = 0
    protected var finished = false
    val logger = Logger(this.getClass.getName)

    context.addTaskCompletionListener { context => closeIfNeeded() }

    val conn: Connection = getConnection()

    val stmt: PreparedStatement = incomingPartition match {
      case teradataJdbcPartition: TeradataJdbcPartition =>
        // Appending the partition specific modulus query onto the incoming sql
        // If the number of partitions is greater than 1, then appending the merge sequence
        val sqlToBeExecuted = if (teradataJdbcPartition.numOfPartitions > 1) {
          PartitionUtils.getMergedPartitionSequence(
            sql,
            teradataJdbcPartition.partitionColumns,
            teradataJdbcPartition.numOfPartitions
          )
        } else {
          sql
        }
        val stmt: PreparedStatement = conn.prepareStatement(
          sqlToBeExecuted, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
        )
        // Setting the current mod based on the partition index
        if(teradataJdbcPartition.numOfPartitions >1 ){
          stmt.setInt(1, teradataJdbcPartition.index)
        }
        logger.info(s"Executing: $sqlToBeExecuted & partition_index -> ${teradataJdbcPartition.index}")
        stmt
      case modifiedJdbcPartition: ModifiedJdbcPartition =>
        logger.info(s"Setting lowerBound = ${modifiedJdbcPartition.lower} &" +
          s"  upperBound=${modifiedJdbcPartition.upper}")
        val stmt: PreparedStatement = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY)
        stmt.setString(1, modifiedJdbcPartition.lower.toString)
        stmt.setString(2, modifiedJdbcPartition.upper.toString)
        logger.info(s"Executing: $sql")
        stmt
    }

    // setting fetchSize
    stmt.setFetchSize(fetchSize)

    val rs: ResultSet = stmt.executeQuery()

    def getNext: T = {
      if (rs.next()) {
        recordsRead+=1
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    def close() {
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      } finally {
        logger.info(s"Records read for partition: ${incomingPartition.index} -> $recordsRead rows")
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }


    def closeIfNeeded() {
      if (!closed) {
        // Note: it's important that we set closed = true before calling close(), since setting it
        // afterwards would permit us to call close() multiple times if close() threw an exception.
        closed = true
        logger.info(s"Closing connection in Executor:$id")
        close()
      }
    }

    override def hasNext: Boolean = {
      if (!finished) {
        if (!gotNext) {
          nextValue = getNext
          if (finished) {
            closeIfNeeded()
          }
          gotNext = true
        }
      }
      !finished
    }

    override def next(): T = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      nextValue
    }
  }
}

object ExtendedJdbcRDD {
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(
      i => {
        val obj = rs.getObject(i + 1)
        obj match {
          case value: JDK6_SQL_Clob =>
            val lines = IOUtils.readLines(value.getAsciiStream)
            String.join(GimelConstants.COMMA, lines)
          case _ => obj
        }
      }
    )
  }

  trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }

}

private class ModifiedJdbcPartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  override def index: Int = idx
}

private case class TeradataJdbcPartition(idx: Int, partitionColumns: Seq[String], numOfPartitions: Int)
  extends Partition {
  override def index: Int = idx
}
