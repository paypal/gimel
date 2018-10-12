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

import java.sql.{BatchUpdateException, Connection, PreparedStatement, ResultSet, SQLException}

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.JdbcRDD

import com.paypal.gimel.jdbc.conf.JdbcConfigs
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
  * @param lowerBound    the minimum value of the first placeholder
  * @param upperBound    the maximum value of the second placeholder
  *                      The lower and upper bounds are inclusive.
  * @param numPartitions the number of partitions.
  *                      Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
  *                      the query would be executed twice, once with (1, 10) and once with (11, 20)
  * @param mapRow        a function from a ResultSet to a single row of the desired result type(s).
  *                      This should only call getInt, getString, etc; the RDD takes care of calling next.
  *                      The default maps a ResultSet to an array of Object.
  */
class ExtendedJdbcRDD[T: ClassTag](
                                    sc: SparkContext,
                                    getConnection: () => Connection,
                                    sql: String,
                                    lowerBound: Long,
                                    upperBound: Long,
                                    numPartitions: Int,
                                    fetchSize: Int,
                                    mapRow: (ResultSet) => T = ExtendedJdbcRDD.resultSetToObjectArray _

                                  )
  extends JdbcRDD[T](sc, getConnection, sql, lowerBound, upperBound, numPartitions, mapRow) with Logging {

  override def getPartitions: Array[Partition] = {
    val logger = Logger(this.getClass.getName)

    // bounds are inclusive, hence the + 1 here and - 1 on end
    val length = BigInt(1) + upperBound - lowerBound
    (0 until numPartitions).map { i =>
      val start = lowerBound + ((i * length) / numPartitions)
      val end = lowerBound + (((i + 1) * length) / numPartitions) - 1
      logger.info(s"Bounds for partition=${i} --> lowerBound=${start},upperBound=${upperBound} ")
      new ModifiedJdbcPartition(i, start.toLong, end.toLong)
    }.toArray

  }


  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new Iterator[T] {

    val logger = Logger(this.getClass.getName)

    private var gotNext = false
    private var nextValue: T = _
    private var closed = false
    protected var finished = false

    context.addTaskCompletionListener { context => closeIfNeeded() }
    val part = thePart.asInstanceOf[ModifiedJdbcPartition]
    val conn = getConnection()

    val stmt: PreparedStatement = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // setting fetchSize
    stmt.setFetchSize(fetchSize)

    // set lowerBound and upperBound
    // NOTE: set lower and upper bounds only when querypushdown is NOT set. i.e, != "true"
    val queryPushDownFlag: String = context.getLocalProperty(JdbcConfigs.jdbcPushDownEnabled)
    logger.info(s"queryPushDownFlag is set to ${queryPushDownFlag}")
    queryPushDownFlag match {
      case "true" =>
        // do nothing

      case _ =>
        logger.info(s"Setting lowerBound = ${part.lower} &  upperBound=${part.upper}")
        stmt.setLong(1, part.lower)
        stmt.setLong(2, part.upper)
    }

    logger.info(s"Executing: ${sql}")
    val rs = stmt.executeQuery()

    def getNext(): T = {
      if (rs.next()) {
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

        logger.info(s"Closing connection in Executor:${id}")
        close()
      }
    }

    override def hasNext: Boolean = {
      if (!finished) {
        if (!gotNext) {
          nextValue = getNext()
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
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }

  trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }

}

private class ModifiedJdbcPartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  override def index: Int = idx
}
