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

import java.sql.{BatchUpdateException, Connection, DriverManager, JDBCType, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}

import scala.collection.immutable.Map

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import com.paypal.gimel.common.conf.{CatalogProviderConfigs, GimelConstants}
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.logger.Logger


/**
  * JDBC implementation internal to PCatalog
  * This implementation will be used to read from any JDBC data sources e.g. MYSQL, TERADATA
  */
object JDBCUtilities {
  def apply(sparkSession: SparkSession): JDBCUtilities = new JDBCUtilities(sparkSession)
}

class JDBCUtilities(sparkSession: SparkSession) extends Serializable {

  // val logger = Logger(this.getClass)
  val authUtilities: JDBCAuthUtilities = JDBCAuthUtilities(sparkSession)

  /**
    * This method reads the data from JDBC data source into dataFrame
    *
    * @param dataset Name of the PCatalog Data Set
    * @param dataSetProps
    *                dataSetprops is the way to set various additional parameters for read and write operations in DataSet class
    *                Example UseCase :
    *                We can specify the additional parameters for databases using JDBC.
    *                e.g. For "Teradata", exporting data, specify "teradata.read.type" parameter as "FASTEXPORT" (optional)
    * @return DataFrame
    */
  def read(dataset: String, dataSetProps: Map[String, Any]): DataFrame = {

    val logger = Logger(this.getClass.getName)
    // logger.setLogLevel("CONSOLE")
    // sparkSession.conf.set("gimel.logging.level", "CONSOLE")

    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)

    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    val jdbcURL = jdbcOptions("url")
    val dbtable = jdbcOptions("dbtable")

    // get connection
    var conn: Connection = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

    // get partitionColumn
    val userPartitionColumn: Option[Any] = dataSetProps.get("partitionColumn")
    val partitionColumn = userPartitionColumn match {

      case None =>
        // get numeric only primary index of the table if not specified
        val primaryIndices: Seq[String] = JdbcAuxiliaryUtilities.getPrimaryKeys(jdbcURL, dbtable, conn, true)
        val defaultPartitionColumn: String = {
          if (!primaryIndices.isEmpty) {
            primaryIndices(0)
          }
          else {
            JdbcConstants.noPartitionColumn
          }
        }
        defaultPartitionColumn

      case _ =>
        userPartitionColumn.get.toString
    }

    // get lowerBound & upperBound
    val (lowerBoundValue: Double, upperBoundValue: Double) = if (!partitionColumn.equals(JdbcConstants.noPartitionColumn)) {
      println(s"Partition column is set to ${partitionColumn}")
      JdbcAuxiliaryUtilities.getMinMax(partitionColumn, dbtable, conn)
    }
    else {
      (0.0, 20.0)
    }

    val lowerBound = dataSetProps.getOrElse("lowerBound", lowerBoundValue.floor.toLong).toString.toLong
    val upperBound = dataSetProps.getOrElse("upperBound", upperBoundValue.ceil.toLong).toString.toLong

    val userSpecifiedPartitions = dataSetProps.getOrElse("numPartitions", JdbcConstants.NUM_READ_PARTITIONS).toString.toInt

    // set number of partitions
    val numPartitions: Int = partitionColumn match {
      case JdbcConstants.noPartitionColumn =>
        println(s"Number of partitions are set to 1 with NO partition column.")
        1
      case _ =>
        if (userSpecifiedPartitions > JdbcConstants.NUM_READ_PARTITIONS) {
          println(s"WARNING: Maximum number of partitions are SET to ${JdbcConstants.NUM_READ_PARTITIONS} due to Teradata connections limitations")
        }
        println(s"Teradata Read for  partitionColumn=${partitionColumn} with lowerBound=${lowerBound} and upperBound=${upperBound}")
        Math.min(userSpecifiedPartitions, JdbcConstants.NUM_READ_PARTITIONS)
    }

    val fetchSize = dataSetProps.getOrElse("fetchSize", JdbcConstants.defaultReadFetchSize).toString.toInt

    val dbConnection = new DbConnection(jdbcConnectionUtility)
    val selectStmt = s"SELECT * FROM ${dbtable} WHERE ?<=$partitionColumn AND $partitionColumn<=?"
    try {

      // set jdbcPushDownFlag to false if using through dataset.read
      logger.info(s"Setting jdbcPushDownFlag to FALSE in TaskContext")
      sparkSession.sparkContext.setLocalProperty(JdbcConfigs.jdbcPushDownEnabled, "false")

      val jdbcRDD: ExtendedJdbcRDD[Array[Object]] = new ExtendedJdbcRDD(sparkSession.sparkContext, dbConnection, selectStmt, lowerBound, upperBound, numPartitions, fetchSize)

      conn = if (conn.isClosed || conn == null) {
        jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
      }
      else {
        conn
      }

      // getting table schema to build final dataframe
      val tableSchema = JdbcReadUtility.resolveTable(jdbcURL, dbtable, conn)
      val rowRDD: RDD[Row] = jdbcRDD.map(v => Row(v: _*))
      sparkSession.createDataFrame(rowRDD, tableSchema)
    }
    catch {
      case exec: SQLException =>
        var ex: SQLException = exec
        while (ex != null) {
          ex.printStackTrace()
          ex = ex.getNextException
        }
        throw exec
    }
    finally {
      // re-setting all configs for read
      JDBCCommons.resetDefaultConfigs(sparkSession)
    }

  }

  // Degrading this READ API
  //  /**
  //    * This function reads data with specified parameters from JDBC datasource table and returns it as a dataFrame
  //    *
  //    * @param dataSetProps dataset properties to specify additional parameters
  //    * @return DataFrame
  //    */
  //  private def readWithJdbc(dataSetProps: Map[String, Any]): DataFrame = {
  //
  //    val jdbcOptions: Map[String, String] = getJDBCOptions(dataSetProps)
  //
  //    // get real user of JDBC
  //    val realUser: String = dataSetProps.getOrElse(JdbcConstants.jdbcUserName, JDBCCommons.getDefaultUser(sparkSession)).toString
  //
  //    // get password strategy for JDBC
  //    val jdbcPasswordStrategy = dataSetProps.getOrElse(JdbcConfigs.jdbcPasswordStrategy, JdbcConstants.jdbcDefaultPasswordStrategy).toString
  //
  //
  //    val conn: Connection = JDBCCommons.getJdbcConnection(jdbcOptions("url"), jdbcOptions("user"), jdbcOptions("password"))
  //    // set QueryBand
  //    JDBCCommons.setQueryBand(conn, realUser, jdbcPasswordStrategy)
  //
  //    // get primary index of the table
  //    val primaryIndices: Seq[String] = getPrimaryKeys(jdbcOptions("dbtable"), conn)
  //    val defaultPartitionColumn: String = {
  //      if (!primaryIndices.isEmpty) {
  //        primaryIndices(0)
  //      }
  //      else {
  //        "10"
  //      }
  //    }
  //    val partitionColumn: String = dataSetProps.getOrElse("partitionColumn", defaultPartitionColumn).toString
  //
  //    val (lowerBoundValue, upperBoundValue) = if (!partitionColumn.equals("10")) {
  //      getMinMax(partitionColumn, jdbcOptions("dbtable"), conn)
  //    }
  //    else {
  //      (0, 20)
  //    }
  //
  //    // close connection
  //    conn.close()
  //
  //    val lowerBound = dataSetProps.getOrElse("lowerBound", lowerBoundValue).toString
  //    val upperBound = dataSetProps.getOrElse("upperBound", upperBoundValue).toString
  //    val numPartitions = dataSetProps.getOrElse("numPartitions", JdbcConstants.NUM_READ_PARTITIONS).toString
  //    val fetchSize = dataSetProps.getOrElse("fetchSize", JdbcConstants.defaultReadFetchSize).toString
  //
  //    // map to specify all JDBC read paramaters
  //    val jdbcREADOptions: Map[String, String] = jdbcOptions +
  //      ("partitionColumn" -> partitionColumn) +
  //      ("lowerBound" -> lowerBound) +
  //      ("upperBound" -> upperBound) +
  //      ("numPartitions" -> numPartitions) +
  //      ("fetchSize" -> fetchSize)
  //
  //    // Read data from JDBC datasource with specified parameters
  //    sparkSession
  //      .read
  //      .format("jdbc")
  //      .options(jdbcREADOptions)
  //      .load()
  //  }


  /**
    * This method writes the dataframe into given JDBC datasource
    *
    * @param dataset      Name of the PCatalog Data Set
    * @param dataFrame    The DataFrame to write into Target table
    * @param dataSetProps Any more options to provide
    * @return DataFrame
    */
  def write(dataset: String, dataFrame: DataFrame, dataSetProps: Map[String, Any]): DataFrame = {

    val logger = Logger(this.getClass.getName)

    // throw exception if spark.speculation is set to true
    // NOTE:  When spark speculation is turned on, a Teradata write task could be potentially blocked due to a deadlock / race condition created by speculative tasks.
    // This is not a permanent fix. In order to avoid race conditions set spark.speculation=false. The exception is thrown so that user is aware of the settings and assumptions while using API.
    val sparkSpeculation: String = sparkSession.sparkContext.getConf.get(GimelConstants.SPARK_SPECULATION)
    if (sparkSpeculation.equalsIgnoreCase("true")) {
      throw new Exception("Unsupported spark configuration for spark.speculation=true. Teradata write API works with spark.speculation=false. Please restart your spark session with spark.speculation=false in your spark configuration")
    }

    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)

    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    var jdbc_url = jdbcOptions("url")
    val batchSize: Int = dataSetProps.getOrElse("batchSize", s"${JdbcConstants.defaultWriteBatchSize}").toString.toInt
    val dbtable = jdbcOptions("dbtable")
    val teradataType: String = dataSetProps.getOrElse(JdbcConfigs.teradataWriteType, "").toString
    val insertStrategy: String = dataSetProps.getOrElse(JdbcConfigs.jdbcInsertStrategy, s"${JdbcConstants.defaultInsertStrategy}").toString

    val userSpecifiedPartitions = dataSetProps.getOrElse("numPartitions", JdbcConstants.NUM_WRITE_PARTITIONS).toString.toInt

    if (userSpecifiedPartitions > JdbcConstants.NUM_WRITE_PARTITIONS) {
      println(s"WARNING: Maximum number of partitions are SET to ${JdbcConstants.NUM_WRITE_PARTITIONS} due to Teradata connections limitations")
    }
    val insertPartitionsCount: Int = Math.min(userSpecifiedPartitions, JdbcConstants.NUM_WRITE_PARTITIONS)

    val partialArgHolder = if (insertStrategy.equalsIgnoreCase("update") || insertStrategy.equalsIgnoreCase("upsert")) {
      // get Set columns for update API
      val setColumns: List[String] = {
        val userSpecifiedSetColumns = dataSetProps.getOrElse(JdbcConfigs.jdbcUpdateSetColumns, null)
        if (userSpecifiedSetColumns == null) {
          dataFrame.columns.toList
        }
        else {
          userSpecifiedSetColumns.toString.split(",").toList
        }
      }

      // get WHERE columns for update API
      val whereColumns: List[String] = {
        val userSpecifiedWhereColumns = dataSetProps.getOrElse(JdbcConfigs.jdbcUpdateWhereColumns, null)
        if (userSpecifiedWhereColumns == null) {
          val primaryKeys = JdbcAuxiliaryUtilities.getPrimaryKeys(jdbc_url, dbtable, jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand(), false)
          primaryKeys
        }
        else {
          userSpecifiedWhereColumns.toString.split(",").toList
        }
      }
      println(s"Setting SET columns: ${setColumns}")
      println(s"Setting WHERE columns: ${whereColumns}")
      JDBCArgsHolder(dataSetProps, dataFrame.columns, jdbc_url, dbtable, _: Int, dataFrame.schema.length, setColumns, whereColumns)
    }
    else {
      JDBCArgsHolder(dataSetProps, dataFrame.columns, jdbc_url, dbtable, _: Int, dataFrame.schema.length)
    }


    if (insertStrategy.equalsIgnoreCase("FullLoad")) {
      val dbconn: Connection = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

      // truncate table
      JdbcAuxiliaryUtilities.truncateTable(dbtable, dbconn)

      dbconn.close()
    }

    val (insetBatchSize, insertMethod, coalesceMethod) = (insertStrategy, teradataType) match {
      case ("update", _) =>
        val dfIdentityFunction = (df: DataFrame) => df.coalesce(insertPartitionsCount.toInt)
        (0, updateTable _, dfIdentityFunction)
      case ("upsert", _) =>
        val dfIdentityFunction = (df: DataFrame) => df.coalesce(insertPartitionsCount.toInt)
        (0, upsertTable _, dfIdentityFunction)
      case (_, _) =>
        // doing coalesce ---- check
        val dfIdentityFunction = (df: DataFrame) => df.coalesce(insertPartitionsCount.toInt)
        (batchSize, insertParallelBatch _, dfIdentityFunction)
    }

    val sourceDataFrame = coalesceMethod(dataFrame)

    val jdbcHolder = partialArgHolder(insetBatchSize)

    try {
      insertMethod(sourceDataFrame, jdbcConnectionUtility, jdbcHolder)
      dataFrame
    }
    catch {
      case exec: Throwable =>
        throw exec
    }
    finally {
      // re-setting all configs for read
      JDBCCommons.resetDefaultConfigs(sparkSession)
    }
  }

  /**
    * This method returns the index of the column in array of columns
    *
    * @param dataFramecolumns Array of columns in dataframe
    * @param column           column for which index has to be found
    * @return Int
    */
  def getColumnIndex(dataFramecolumns: Array[String], column: String): Int = {
    dataFramecolumns.indexWhere(column.equalsIgnoreCase)
  }


  /**
    * It cooks the given preparedStatement with the given row.
    * It uses the columns whose index are given in `columnIndices` and sets parameters at indeices
    * given in `paramIndices`.
    *
    * @param st
    * @param row
    * @param columnIndices
    * @param paramIndices
    */
  private def cookStatementWithRow(st: PreparedStatement, row: Row,
                                   columnIndices: Seq[Int],
                                   paramIndices: Seq[Int]): PreparedStatement = {
    require(columnIndices.size == paramIndices.size,
      s"Different number of column and param indices were passed to $st.")
    columnIndices.zip(paramIndices).foreach { case (columnIndex, paramIndex) =>
      val columnValue = row.get(columnIndex)
      val targetSqlType = SparkToJavaConverter.getSQLType(row.schema(columnIndex).dataType)
      st.setObject(paramIndex, columnValue, targetSqlType)
    }
    st
  }


  /**
    * This method inserts into given table in given mode
    *
    * @param dataFrame
    * @param jdbcConnectionUtility
    * @param jdbcHolder
    */
  private def insertParallelBatch(dataFrame: DataFrame, jdbcConnectionUtility: JDBCConnectionUtility, jdbcHolder: JDBCArgsHolder) {

    // create a JDBC connection to get DDL of target table
    var driverCon = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

    // get DDL of target table
    val ddl = JdbcAuxiliaryUtilities.getDDL(jdbcHolder.dbTable, driverCon).toUpperCase
    driverCon.close()

    // For each partition create a temp table to insert
    dataFrame.foreachPartition { batch =>

      // create logger inside the executor
      val logger = Logger(this.getClass.getName)

      // get the partition ID
      val partitionID = TaskContext.getPartitionId()

      // creating a new connection for every partition
      // NOTE: Here, singleton connection is replaced by java.sql.Connection creating a separate connection for every partition.
      // Singleton connection connection needs to be correctly verified within multiple cores.
      var dbc = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

      val partitionTableName = s"${jdbcHolder.dbTable.toUpperCase}_${JdbcConstants.GIMEL_TEMP_PARTITION}_${partitionID}"

      // first drop the temp table, if exists
      JdbcAuxiliaryUtilities.dropTable(partitionTableName, dbc)

      try {

        // create JDBC temp table
        val tempTableDDL = ddl.replace(jdbcHolder.dbTable.toUpperCase, partitionTableName)

        logger.info(s"Creating temptable: ${partitionTableName}")

        // create a temp partition table
        JdbcAuxiliaryUtilities.executeQuerySatement(tempTableDDL, dbc)
      }
      catch {
        case ex =>
          logger.info(s"Creation of temptable: ${partitionTableName} failed")
          ex.printStackTrace()
          throw ex
      }

      // close the connection, if batch is empty, so that we don't hold connection
      if (batch.isEmpty) {
        dbc.close()
      }

      if (batch.nonEmpty) {

        val maxBatchSize = math.max(1, jdbcHolder.batchSize)
        try {

          // check if connection is closed or null
          if (dbc.isClosed || dbc == null) {
            dbc = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
          }

          val st = dbc.prepareStatement(JdbcAuxiliaryUtilities.getInsertStatement(partitionTableName, jdbcHolder.cols))

          logger.info(s"Inserting to temptable ${partitionTableName} of Partition: ${partitionID}")

          // set AutoCommit to FALSE
          dbc.setAutoCommit(false)

          var count = 0
          var rowCount = 0

          batch.sliding(maxBatchSize, maxBatchSize).foreach { rows =>
            rows.foreach { row =>
              cookStatementWithRow(st, row, 0 until row.schema.length, 1 to row.schema.length)
              st.addBatch()
              rowCount = rowCount + 1
            }
            try {
              st.executeBatch()
              count = count + 1
            }
            catch {
              case exec: Throwable => {
                exec match {
                  case batchExc: BatchUpdateException =>
                    logger.info(s"Exception in inserting data into ${partitionTableName} of Partition: ${partitionID}")
                    var ex: SQLException = batchExc
                    while (ex != null) {
                      ex.printStackTrace()
                      ex = ex.getNextException
                    }
                    throw batchExc
                  case _ =>
                    logger.info(s"Exception in inserting data into ${partitionTableName} of Partition: ${partitionID}")
                    throw exec
                }
              }
            }
          }
          logger.info(s"Data Insert successful into ${partitionTableName} of Partition: ${partitionID}")

          // commit
          dbc.commit()

          // close the connection
          dbc.close()

        }
        catch {
          case exec: Throwable =>
            exec.printStackTrace()
            throw exec
        }
        finally {
          // check if any connection open inside executor and explicitly close it
          if (!dbc.isClosed && dbc != null) {
            dbc.close()
          }
        }
      }
    }

    // create a JDBC connection to get DDL of target table
    driverCon = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

    // Now union all the temp partition tables and insert into target table
    try {
      // now insert all partitions into target table
      JdbcAuxiliaryUtilities.insertPartitionsIntoTargetTable(jdbcHolder.dbTable, driverCon, dataFrame.toJavaRDD.getNumPartitions)

      // now drop all the temp tables created by executors
      JdbcAuxiliaryUtilities.dropAllPartitionTables(jdbcHolder.dbTable, driverCon, dataFrame.toJavaRDD.getNumPartitions)
      driverCon.close()
    }
    catch {
      case e: Throwable => e.printStackTrace()

        // get or create a JDBC connection to get DDL of target table
        val driverCon = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
        // now drop all the temp tables created by executors
        JdbcAuxiliaryUtilities.dropAllPartitionTables(jdbcHolder.dbTable, driverCon, dataFrame.toJavaRDD.getNumPartitions)
        driverCon.close()
        throw e
    }
    finally {
      // check if any connection open inside executor and explicitly close it
      if (!driverCon.isClosed && driverCon != null) {
        driverCon.close()
      }
    }
  }

  /**
    * This method is used to UPDATE into the table
    *
    * @param dataFrame  dataFrame to be loaded into table
    * @param jdbcHolder jdbc arguments required for writing into table
    */
  private def updateTable(dataFrame: DataFrame, jdbcConnectionUtility: JDBCConnectionUtility, jdbcHolder: JDBCArgsHolder) {

    dataFrame.foreachPartition { batch =>
      if (batch.nonEmpty) {

        // creating a new connection for every partition
        // NOTE: Here, singleton connection is replaced by java.sql.Connection creating a separate connection for every partition.
        // Singleton connection connection needs to be correctly verified within multiple cores.
        val dbc = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()

        try {
          val updateStatement = JdbcAuxiliaryUtilities.getUpdateStatement(jdbcHolder.dbTable, jdbcHolder.setColumns, jdbcHolder.whereColumns)
          val st: PreparedStatement = dbc.prepareStatement(updateStatement)
          val maxBatchSize = math.max(1, jdbcHolder.batchSize)
          val setColumnIndices = jdbcHolder.setColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
          val whereColumnIndices = jdbcHolder.whereColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
          batch.sliding(maxBatchSize, maxBatchSize).foreach { rows =>
            rows.foreach { row =>
              cookStatementWithRow(st, row, setColumnIndices, 1 to setColumnIndices.length)
              // set the condition variables
              val whereColumnIndicesUpdated = (1 to whereColumnIndices.length).map(_ + setColumnIndices.length)
              cookStatementWithRow(st, row, whereColumnIndices, whereColumnIndicesUpdated)
              st.addBatch()
            }
            try {
              st.executeBatch()
            }
            catch {
              case exec: Throwable => {
                exec match {
                  case batchExc: BatchUpdateException =>
                    var ex: SQLException = batchExc
                    while (ex != null) {
                      ex.printStackTrace()
                      ex = ex.getNextException
                    }
                    throw batchExc
                  case _ =>
                    throw exec
                }
              }
            }
          }
        }
        catch {
          case exec: Throwable =>
            exec.printStackTrace()
            throw exec
        }
        finally {
          // check if any connection open inside executor and explicitly close it
          if (!dbc.isClosed && dbc != null) {
            dbc.close()
          }
        }

      }
    }
  }

  /**
    * This method is used to UPSERT into the table
    *
    * @param dataFrame
    * @param jDBCConnectionUtility
    * @param jdbcHolder
    */
  private def upsertTable(dataFrame: DataFrame, jDBCConnectionUtility: JDBCConnectionUtility, jdbcHolder: JDBCArgsHolder) {
    dataFrame.foreachPartition { batch =>
      if (batch.nonEmpty) {
        val dbc = jDBCConnectionUtility.getJdbcConnectionAndSetQueryBand()
        try {
          val numCols: Int = jdbcHolder.cols
          val updateStatement = JdbcAuxiliaryUtilities.getUpdateStatement(jdbcHolder.dbTable, jdbcHolder.setColumns, jdbcHolder.whereColumns)
          val st: PreparedStatement = dbc.prepareStatement(updateStatement)
          val setColumnIndices = jdbcHolder.setColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
          val whereColumnIndices = jdbcHolder.whereColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
          batch.foreach { row =>
            cookStatementWithRow(st, row, setColumnIndices, 1 to setColumnIndices.length)
            // set the condition variables
            val whereColumnIndicesUpdated = (1 to whereColumnIndices.length).map(_ + setColumnIndices.length)
            cookStatementWithRow(st, row, whereColumnIndices, whereColumnIndicesUpdated)
            // update table
            val updateResult: Int = st.executeUpdate()
            // if no update, then insert into table
            if (updateResult == 0) {
              val st = dbc.prepareStatement(JdbcAuxiliaryUtilities.getInsertStatement(jdbcHolder.dbTable, numCols))
              cookStatementWithRow(st, row, 0 until row.schema.length, 1 to row.schema.length)
              // insert into table
              try {
                st.executeUpdate()
              }
              catch {
                case exec: Throwable => {
                  exec match {
                    case batchExc: BatchUpdateException =>
                      var ex: SQLException = batchExc
                      while (ex != null) {
                        ex.printStackTrace()
                        ex = ex.getNextException
                      }
                      throw batchExc
                    case _ =>
                      throw exec
                  }
                }
              }
            }
          }
        }
        catch {
          case exec: Throwable =>
            exec.printStackTrace()
            throw exec
        }
        finally {
          // check if any connection open inside executor and explicitly close it
          if (!dbc.isClosed && dbc != null) {
            dbc.close()
          }
        }
      }
    }
  }

}

/**
  * case class to define column
  *
  * @param columName
  * @param columnType
  */
case class Column(columName: String, columnType: String)
