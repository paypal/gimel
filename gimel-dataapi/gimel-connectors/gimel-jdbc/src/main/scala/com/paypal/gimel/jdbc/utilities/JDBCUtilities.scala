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

import java.io.{PrintWriter, StringWriter}
import java.sql.{Connection, PreparedStatement, SQLException, SQLWarning}
import java.time.Instant

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructField

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{CatalogProviderConfigs, GimelConstants}
import com.paypal.gimel.common.utilities.{DataSetUtils, GenericUtils}
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.exception._
import com.paypal.gimel.jdbc.utilities.JdbcAuxiliaryUtilities.getJDBCSystem
import com.paypal.gimel.logger.Logger

/**
  * JDBC implementation internal to PCatalog
  * This implementation will be used to read from any JDBC data sources e.g. MYSQL, TERADATA
  */
object JDBCUtilities {

  val DEF_LOWER_BOUND : Long = 0
  val DEF_UPPER_BOUND : Long = 20

  def getOrCreateConnection(jdbcConnectionUtility: JDBCConnectionUtility,
                            conn: Option[Connection] = None,
                            logger: Option[Logger] = None): Connection = {
    if (conn.isEmpty || conn.get.isClosed) {
      jdbcConnectionUtility.jdbcSystem match {
        case JdbcConstants.TERADATA =>
          Class.forName("com.teradata.jdbc.TeraDriver")
        case _ =>
          // scalastyle:off println
          println(s"Not loading the driver class for ${jdbcConnectionUtility.jdbcSystem} !!")
        // scalastyle:on println
      }
      jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand(logger)
    } else {
      conn.get
    }
  }

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
    import JDBCUtilities._
    import PartitionUtils._

    val logger = Logger(this.getClass.getName)
    // logger.setLogLevel("CONSOLE")
    // sparkSession.conf.set("gimel.logging.level", "CONSOLE")

    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    val jdbcURL = jdbcOptions(JdbcConfigs.jdbcUrl)
    val dbtable = jdbcOptions(JdbcConfigs.jdbcDbTable)
    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps ++ jdbcOptions)
    logger.info(s"Received JDBC options: $jdbcOptions and dataset options: $dataSetProps")

    // get connection
    val conn: Connection = getOrCreateConnection(jdbcConnectionUtility)
    val userSpecifiedFetchSize = dataSetProps.getOrElse("fetchSize", JdbcConstants.DEFAULT_READ_FETCH_SIZE).toString.toInt

    val jdbcSystem = getJDBCSystem(jdbcURL)
    try {
      jdbcSystem match {
        case JdbcConstants.TERADATA =>
          val selectStmt = s"SELECT * FROM $dbtable"

          val mutableJdbcOptions: mutable.Map[String, String] = scala.collection.mutable.Map(jdbcOptions.toSeq: _*)

          // get the partition columns
          val partitionColumns: Seq[String] = JdbcAuxiliaryUtilities.getAndSetPartitionParameters(
            sparkSession, dataSetProps, userSpecifiedFetchSize, mutableJdbcOptions, conn)

          // Get connection details per the explain plan of the incomingSql
          val (connectionDetails, connectionUtilityPerIncomingSQL) =
            JdbcAuxiliaryUtilities.getConnectionInfo(sparkSession, jdbcConnectionUtility,
              dataSetProps, selectStmt, Some(logger), partitionColumns)

          val jdbcRDD: ExtendedJdbcRDD[Array[Object]] = new ExtendedJdbcRDD(sparkSession.sparkContext,
            new DbConnection(connectionUtilityPerIncomingSQL), selectStmt, connectionDetails.fetchSize,
            PartitionInfoWrapper(JdbcConstants.TERADATA, partitionColumns, JdbcConstants.DEFAULT_LOWER_BOUND,
              JdbcConstants.DEFAULT_UPPER_BOUND, numOfPartitions = connectionDetails.numOfPartitions))

          // getting table schema to build final dataframe
          val tableSchema = JdbcReadUtility.resolveTable(jdbcURL, selectStmt,
            getOrCreateConnection(jdbcConnectionUtility, Some(conn)))
          val rowRDD: RDD[Row] = jdbcRDD.map(v => Row(v: _*))
          sparkSession.createDataFrame(rowRDD, tableSchema)
        case _ =>
          // get partitionColumns
          val partitionOptions = if (dataSetProps.contains(JdbcConfigs.jdbcPartitionColumns)) {
            jdbcOptions + (JdbcConfigs.jdbcPartitionColumns -> dataSetProps(JdbcConfigs.jdbcPartitionColumns).toString)
          } else jdbcOptions
          val partitionColumns = PartitionUtils.getPartitionColumns(partitionOptions, conn, numericOnlyFlag = true)
          val (partitionColumn, lowerBoundValue, upperBoundValue, nofOfPartitions) = if (partitionColumns.nonEmpty) {
            Try(JdbcAuxiliaryUtilities.getMinMax(partitionColumns.head, dbtable, conn)) match {
              case Success((lowerBound, upperBound)) =>
                // set number of partitions
                val userSpecifiedPartitions = dataSetProps.get("numPartitions")
                val numPartitions: Int = JdbcAuxiliaryUtilities.getNumPartitions(jdbcURL, userSpecifiedPartitions,
                  JdbcConstants.READ_OPERATION)
                logger.info(s"Partition column is set to ${partitionColumns.head} " +
                  s"and no of partitions: $numPartitions")
                (Some(partitionColumns.head), GenericUtils.parseLong(dataSetProps.getOrElse("lowerBound",
                  lowerBound).toString).getOrElse(JDBCUtilities.DEF_LOWER_BOUND),
                  GenericUtils.parseLong(dataSetProps.getOrElse("upperBound",
                    upperBound).toString).getOrElse(JDBCUtilities.DEF_UPPER_BOUND), numPartitions)
              case Failure(_) => (None, JDBCUtilities.DEF_LOWER_BOUND, JDBCUtilities.DEF_UPPER_BOUND, 1)
            }
          } else {
            (None, JDBCUtilities.DEF_LOWER_BOUND, JDBCUtilities.DEF_UPPER_BOUND, 1)
          }

          // default spark JDBC read
          JdbcAuxiliaryUtilities.sparkJdbcRead(sparkSession, jdbcURL, dbtable,
            partitionColumn, lowerBoundValue, upperBoundValue, nofOfPartitions, userSpecifiedFetchSize,
            jdbcConnectionUtility.getConnectionProperties)
      }
    }
    catch {
      case throwable: Throwable =>
        handleException(logger, throwable, s"Exception in reading dataset: $dataset ")
    }
    finally {
      // re-setting all configs for read
      JDBCCommons.resetReadConfigs(sparkSession)
    }
  }


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
    logger.info(s"In JDBC write, received Dataset[$dataset] properties: $dataSetProps")
    // Getting connection info from dataset properties else from the incoming properties
    val jdbcConnectionOptions = DataSetUtils.getJdbcConnectionOptionsFromDataset(dataset, dataSetProps)
    logger.info(s"received JDBC options from dataset[$dataset]: $jdbcConnectionOptions")
    val mergedJdbcOptions = JdbcAuxiliaryUtilities.mergeJDBCOptions(dataSetProps, jdbcConnectionOptions)
    logger.info(s"received merged JDBC options from datasetProps[$dataset]: $mergedJdbcOptions")

    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession,
      if (mergedJdbcOptions.nonEmpty) {
        mergedJdbcOptions
      } else {
        dataSetProps ++ jdbcConnectionOptions
      }
    )
    val jdbc_url = mergedJdbcOptions(JdbcConfigs.jdbcUrl)

    val batchSize: Int = Try(
      dataSetProps.getOrElse("batchSize", s"${JdbcConstants.DEFAULT_WRITE_BATCH_SIZE}").toString.toInt
    ).getOrElse(JdbcConstants.DEFAULT_WRITE_BATCH_SIZE)
    val dbtable = mergedJdbcOptions(JdbcConfigs.jdbcDbTable)

    val insertStrategy: String = dataSetProps.getOrElse(JdbcConfigs.jdbcInsertStrategy,
      s"${JdbcConstants.DEFAULT_INSERT_STRATEGY}").toString

    val partialArgHolder: Int => JDBCArgsHolder = extractJdbcArgsHolder(dataFrame, dataSetProps, logger,
      jdbcConnectionUtility, jdbc_url, dbtable, insertStrategy)

    if (insertStrategy.equalsIgnoreCase("FullLoad")) {
      logger.info(s"Truncating the table :$dbtable as part of the FullLoad strategy")
      // truncate table
      JDBCConnectionUtility.withResources(jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()) {
        connection => JdbcAuxiliaryUtilities.truncateTable(jdbc_url, dbtable, connection, Some(logger))
      }
    } else {
      // Validate target table availability
      JdbcAuxiliaryUtilities.isTargetTableAvailable(dbtable, jdbcConnectionUtility, Some(logger))
    }

    val teradataType: String = dataSetProps.getOrElse(JdbcConfigs.teradataWriteType, "").toString
    val (insetBatchSize, insertMethod) = (insertStrategy, teradataType) match {
      case ("update", _) =>
        (0, updateTable _)
      case ("upsert", _) =>
        (0, upsertTable _)
      case (_, _) =>
        (batchSize, insertParallelBatch _)
    }

    // get number of user partitions based on the type of connection utility[TD: FastLoad or FastExport or JDBC] used
    val userSpecifiedPartitions = dataSetProps.get("numPartitions")
    val sourceDataFrame = JdbcAuxiliaryUtilities.getPartitionedDataFrame(jdbc_url, dataFrame, userSpecifiedPartitions)

    try {
      val jdbcHolder = partialArgHolder(insetBatchSize)
      insertMethod(sourceDataFrame, jdbcConnectionUtility, jdbcHolder)
      dataFrame
    }
    catch {
      case exec: Throwable =>
        throw exec
    }
    finally {
      // re-setting all configs for write
      JDBCCommons.resetWriteConfigs(sparkSession)
    }
  }

  private def extractJdbcArgsHolder(dataFrame: DataFrame, dataSetProps: Map[String, Any],
                                    logger: Logger, jdbcConnectionUtility: JDBCConnectionUtility,
                                    jdbc_url: String, dbtable: String,
                                    insertStrategy: String): Int => JDBCArgsHolder = {
    // Get specific URL properties
    // get real user of JDBC
    val realUser: String = JDBCCommons.getJdbcUser(dataSetProps, sparkSession)

    // get password strategy for JDBC
    val jdbcPasswordStrategy = dataSetProps.getOrElse(JdbcConfigs.jdbcPasswordStrategy,
      JdbcConstants.JDBC_DEFAULT_PASSWORD_STRATEGY).toString

    if (insertStrategy.equalsIgnoreCase(
      "update") || insertStrategy.equalsIgnoreCase("upsert")) {
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
          JDBCConnectionUtility.withResources(jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()) {
            connection =>
              JdbcAuxiliaryUtilities.getPrimaryKeys(jdbc_url, dbtable, connection)
          }
        }
        else {
          userSpecifiedWhereColumns.toString.split(",").toList
        }
      }
      logger.info(s"Setting SET columns: $setColumns")
      logger.info(s"Setting WHERE columns: $whereColumns")
      JDBCArgsHolder(dataSetProps, dataFrame.columns, jdbc_url, realUser, jdbcPasswordStrategy, dbtable, _ : Int,
        dataFrame.schema.length, setColumns, whereColumns)
    } else {
      JDBCArgsHolder(dataSetProps, dataFrame.columns, jdbc_url, realUser, jdbcPasswordStrategy,
        dbtable, _ : Int, dataFrame.schema.length)
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
      try {
        st.setObject(paramIndex, columnValue, targetSqlType)
      }
      catch {
        case e: Throwable =>
          e.printStackTrace()
          val msg =
            s"""
               |Error setting column value=$columnValue at column index=$columnIndex
               |with input dataFrame column dataType=${row.schema(columnIndex).dataType}
               |to target dataType=$targetSqlType
            """.stripMargin
          throw new SetColumnObjectException(msg, e)

      }
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
  private def insertParallelBatch(dataFrame: DataFrame, jdbcConnectionUtility: JDBCConnectionUtility,
                                  jdbcHolder: JDBCArgsHolder) {
    import JDBCConnectionUtility.withResources
    val jdbcSystem = JdbcAuxiliaryUtilities.getJDBCSystem(jdbcHolder.jdbcURL)
    var ddl = ""
    // create logger inside the driver
    val driverLogger = Option(Logger(this.getClass.getName))

    jdbcSystem match {
      case JdbcConstants.TERADATA =>
        // create a JDBC connection to get DDL of target table
        withResources(jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand(driverLogger)) {
          connection =>
            // get DDL of target table
            ddl = JdbcAuxiliaryUtilities.getDDL(jdbcHolder.jdbcURL, jdbcHolder.dbTable, connection)
        }
      case _ => // do nothing
    }


    // For each partition create a temp table to insert
    dataFrame.foreachPartition { batch =>

      // create logger inside the executor
      val logger = Logger(this.getClass.getName)

      // get the partition ID
      val partitionID = TaskContext.getPartitionId()

      // creating a new connection for every partition
      // NOTE: Here, singleton connection is replaced by java.sql.Connection creating a separate connection
      // for every partition.
      // Singleton connection connection needs to be correctly verified within multiple cores.
      val dbc = JdbcAuxiliaryUtilities.createConnectionWithPreConfigsSet(jdbcConnectionUtility,
        jdbcHolder, logger = Some(logger))

      val partitionTableName = jdbcSystem match {
        case JdbcConstants.TERADATA =>
          val partitionTableName = JdbcAuxiliaryUtilities.getPartitionTableName(jdbcHolder.jdbcURL,
            jdbcHolder.dbTable, dbc, partitionID)

          // first drop the temp table, if exists
          JdbcAuxiliaryUtilities.dropTable(partitionTableName, dbc)

          try {
            // create JDBC temp table
            val tempTableDDL = ddl.replace(jdbcHolder.dbTable, partitionTableName)
            logger.info(s"Creating temp table: $partitionTableName with DDL = $tempTableDDL")
            // create a temp partition table
            JdbcAuxiliaryUtilities.executeQueryStatement(tempTableDDL, dbc, recordTimeTakenToExecute = true)
          }
          catch {
            case ex: Throwable =>
              val msg = s"Failure creating temp partition table $partitionTableName"
              logger.error(msg + "\n" + s"${ex.toString}")
              ex.addSuppressed(new JDBCPartitionException(msg))
              throw ex
          }
          partitionTableName
        case _ =>
          jdbcHolder.dbTable
      }

      // close the connection, if batch is empty, so that we don't hold connection
      if (batch.isEmpty) {
        dbc.close()
      } else {
        logger.info(s"Inserting into $partitionTableName")
        val maxBatchSize = math.max(1, jdbcHolder.batchSize)
        withResources {
          JdbcAuxiliaryUtilities.createConnectionWithPreConfigsSet(jdbcConnectionUtility,
            jdbcHolder, connection = Option(dbc))
        } { connection =>
          withResources {
            connection.prepareStatement(JdbcAuxiliaryUtilities.getInsertStatement(partitionTableName, jdbcHolder.cols))
          } {
            statement =>
            {
              // Handle connection warnings
              handleWarning(connection.getWarnings, logger)

              val start = Instant.now().toEpochMilli
              var end = start
              connection.setAutoCommit(false)
              var batchCount = 0
              var rowCount = 0
              var startRowCount = rowCount
              batch.sliding(maxBatchSize, maxBatchSize).foreach {
                rows =>
                  startRowCount = rowCount
                  rows.foreach { row =>
                    cookStatementWithRow(statement, row, 0 until row.schema.length,
                      1 to row.schema.length)
                    statement.addBatch()
                    rowCount += 1
                  }
                  try {
                    val recordsUpserted: Array[Int] = statement.executeBatch()
                    if(statement.getWarnings != null){
                      // Handle statement warnings
                      handleWarning(statement.getWarnings, logger)
                    }
                    end = Instant.now().toEpochMilli
                    batchCount += 1
                    logger.info(s"Total time taken to insert [${Try(recordsUpserted.length).getOrElse(0)} records " +
                      s"with (start_row: $startRowCount & end_row: $rowCount and diff ${rowCount - startRowCount} " +
                      s"records)] for the batch[Batch no: $batchCount & Partition: $partitionTableName] : ${
                        DurationFormatUtils.formatDurationWords(
                          end - start, true, true
                        )
                      }")
                  }
                  catch {
                    case exec: Throwable =>
                      handleException(logger, exec, s"Exception in inserting data into $partitionTableName " +
                        s"of Partition: $partitionID")
                  }
              }
              // commit per batch
              dbc.commit()
              logger.info(s"Successfully inserted into $partitionTableName of Partition: $partitionID " +
                s"with $rowCount rows and $batchCount batches," +
                s" overall time taken -> ${
                  DurationFormatUtils.formatDurationWords(
                    end - start, true, true
                  )
                } ")
              // Connection will be closed as part of JDBCConnectionUtility.withResources
            }
          }
        }
      }
    }



    jdbcSystem match {
      case JdbcConstants.TERADATA =>
        try {
          withResources(JdbcAuxiliaryUtilities.createConnectionWithPreConfigsSet(jdbcConnectionUtility, jdbcHolder)) {
            connection =>

              // Now union all the temp partition tables and insert into target table
              // and insert all partitions into target table
              JdbcAuxiliaryUtilities.insertPartitionsIntoTargetTable(jdbcHolder.dbTable,
                connection, dataFrame.toJavaRDD.getNumPartitions, driverLogger)

              // now drop all the temp tables created by executors
              JdbcAuxiliaryUtilities.dropAllPartitionTables(jdbcHolder.dbTable,
                connection, dataFrame.toJavaRDD.getNumPartitions, driverLogger)
          }
        } catch {
          case e: Throwable =>
            withResources {
              JdbcAuxiliaryUtilities.createConnectionWithPreConfigsSet(jdbcConnectionUtility, jdbcHolder)
            } { connection =>
              // now drop all the temp tables created by executors
              JdbcAuxiliaryUtilities.dropAllPartitionTables(jdbcHolder.dbTable,
                connection, dataFrame.toJavaRDD.getNumPartitions, driverLogger)
            }
            // scalastyle:off printStackTrace
            e.printStackTrace()
            // scalastyle:on printStackTrace
            throw e
        }
      case _ => // do nothing
    }
  }

  private def handleException(logger: Logger, exec: Throwable, errorMessage: String): Nothing = {
    exec match {
      case batchExc: SQLException =>
        logger.info(errorMessage)
        val errors = new mutable.StringBuilder()
        var ex: SQLException = batchExc
        var lastException: SQLException = batchExc
        while (ex != null) {
          if (errors.nonEmpty) {
            errors.append(s"${GimelConstants.COMMA} ")
          }
          errors.append(s = s"SQL state: ${ex.getSQLState} - Error code: ${ex.getErrorCode.toString}")
          lastException = ex
          ex = ex.getNextException
        }
        if (lastException != null) {
          // scalastyle:off printStackTrace
          lastException.printStackTrace()
          // scalastyle:on printStackTrace
        }
        logger.info(s"SQLException: Error codes -> ${errors.toString()}")
        logger.error(batchExc)
        throw lastException
      case _ =>
        logger.error(errorMessage)
        throw exec
    }
  }

  private def handleWarning(warning: SQLWarning, logger: Logger): Unit = {
    var w = warning
    while (w != null) {
      val sw: StringWriter = new StringWriter()
      w.printStackTrace(new PrintWriter(sw, true))
      logger.info("WARNING: - SQL State = " + w.getSQLState + ", Error Code = " + w.getErrorCode + "\n" + sw.toString)
      w = w.getNextWarning
    }
  }

  /**
    * This method is used to UPDATE into the table
    *
    * @param dataFrame  dataFrame to be loaded into table
    * @param jdbcHolder jdbc arguments required for writing into table
    */
  private def updateTable(dataFrame: DataFrame, jdbcConnectionUtility: JDBCConnectionUtility,
                          jdbcHolder: JDBCArgsHolder) {
    dataFrame.foreachPartition { batch =>
      if (batch.nonEmpty) {
        // create logger inside the executor
        val logger = Logger(this.getClass.getName)

        // creating a new connection for every partition
        // NOTE: Here, singleton connection is replaced by java.sql.Connection creating a separate connection
        // for every partition. Singleton connection connection needs to be correctly verified within multiple cores.
        import JDBCConnectionUtility.withResources
        withResources(jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()) {
          dbc =>
            JdbcAuxiliaryUtilities.executePreConfigs(jdbcHolder.jdbcURL, jdbcHolder.dbTable, dbc)
            try {
              val updateStatement = JdbcAuxiliaryUtilities.getUpdateStatement(jdbcHolder.dbTable,
                jdbcHolder.setColumns, jdbcHolder.whereColumns)
              withResources(dbc.prepareStatement(updateStatement)) {
                st =>
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
                      case throwable: Throwable =>
                        handleException(logger, throwable,
                          s"Exception occured while updating table in executeBatch : ${jdbcHolder.dbTable} ")
                    }
                  }
              }
            }
            catch {
              case throwable: Throwable =>
                handleException(logger, throwable,
                  s"Exception occured while updating table in executeBatch : ${jdbcHolder.dbTable} ")
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
  private def upsertTable(dataFrame: DataFrame, jDBCConnectionUtility: JDBCConnectionUtility,
                          jdbcHolder: JDBCArgsHolder) {
    dataFrame.foreachPartition { batch =>
      // create logger inside the executor
      val logger = Logger(this.getClass.getName)
      if (batch.nonEmpty) {
        import JDBCConnectionUtility.withResources
        withResources(jDBCConnectionUtility.getJdbcConnectionAndSetQueryBand()) {
          dbc => {
            JdbcAuxiliaryUtilities.executePreConfigs(jdbcHolder.jdbcURL, jdbcHolder.dbTable, dbc)
            try {
              val numCols: Int = jdbcHolder.cols
              val updateStatement = JdbcAuxiliaryUtilities.getUpdateStatement(jdbcHolder.dbTable,
                jdbcHolder.setColumns, jdbcHolder.whereColumns)
              withResources(dbc.prepareStatement(updateStatement)) {
                updateStatement =>
                  val setColumnIndices = jdbcHolder.setColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
                  val whereColumnIndices = jdbcHolder.whereColumns.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
                  batch.foreach { row =>
                    cookStatementWithRow(updateStatement, row, setColumnIndices, 1 to setColumnIndices.length)
                    // set the condition variables
                    val whereColumnIndicesUpdated = (1 to whereColumnIndices.length).map(_ + setColumnIndices.length)
                    cookStatementWithRow(updateStatement, row, whereColumnIndices, whereColumnIndicesUpdated)
                    // update table
                    val updateResult: Int = updateStatement.executeUpdate()
                    // if no update, then insert into table
                    if (updateResult == 0) {
                      withResources(dbc.prepareStatement(
                        JdbcAuxiliaryUtilities.getInsertStatement(jdbcHolder.dbTable, numCols))
                      ) {
                        insertStatement =>
                          cookStatementWithRow(insertStatement, row, 0 until row.schema.length,
                            1 to row.schema.length)
                          // insert into table
                          try {
                            insertStatement.executeUpdate()
                          }
                          catch {
                            case throwable: Throwable =>
                              handleException(logger, throwable,
                                s"Exception occured while inserting[part of upsert] into table: ${jdbcHolder.dbTable} ")
                          }
                      }
                    }
                  }
              }
            } catch {
              case throwable: Throwable =>
                handleException(logger, throwable,
                  s"Exception occured while upserting table: ${jdbcHolder.dbTable} ")
            }
          }
        }
      }
    }
  }


  /** This method creates the table in teradata database
    *
    * @param dataset      dataset name
    * @param dataSetProps dataset properties
    * @return Boolean
    */
  def create(dataset: String, dataSetProps: Map[String, Any]): Boolean = {
    val logger = Logger(this.getClass.getName)
    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)
    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    val actualProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val catalogProvider = dataSetProps(CatalogProviderConfigs.CATALOG_PROVIDER).toString
    val sql = dataSetProps(GimelConstants.TABLE_SQL).toString
    logger.info("sql statement " + sql)
    val createTableStatement = catalogProvider match {
      case com.paypal.gimel.common.conf
             .CatalogProviderConstants.PCATALOG_PROVIDER | com.paypal.gimel.common.conf
             .CatalogProviderConstants.UDC_PROVIDER =>
        dataSetProps(GimelConstants.CREATE_STATEMENT_IS_PROVIDED) match {
          case "true" =>
            // Since create statement is provided, we do not need to prepare the statement instead we need
            // to pass the sql as is to the teradata engine.
            sql
          case _ =>
            // As the create statement is not provided we need to infer schema from the dataframe.
            // In GimelQueryProcesser we already infer schema and pass the columns with their data types
            // we need construct the create statement
            JDBCUtilityFunctions.prepareCreateStatement(sql, jdbcOptions(JdbcConfigs.jdbcDbTable), dataSetProps)
        }
      case GimelConstants.USER =>
        val colList: Array[String] = actualProps.fields.map(x => (x.fieldName + " " + (x.fieldType) + ","))
        val conCatColumns = colList.mkString("")
        s"""CREATE TABLE ${jdbcOptions(JdbcConfigs.jdbcDbTable)} (${conCatColumns.dropRight(1)} ) """
    }
    val con = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
    val columnStatement: PreparedStatement = con.prepareStatement(createTableStatement)
    columnStatement.execute()
  }

  /**
    * prepareCreateStatement - From the column details passed in datasetproperties from GimelQueryProcesser,
    * create statement is constructed. If user passes primary index , set or multi set table, those will be added in the create statement
    *
    * @param dbtable      - Table Name
    * @param dataSetProps - Data set properties
    * @return - the created prepared statement for creating the table
    */
  def prepareCreateStatement(sql: String, dbtable: String, dataSetProps: Map[String, Any]): String = {
    // Here we remove the SELECT portion and have only the CREATE portion of the DDL supplied so that we can use that to create the table
    val sqlParts = sql.split(" ")
    val lenPartKeys = sqlParts.length
    val index = sqlParts.indexWhere(_.toUpperCase() == "SELECT")
    val createOnly: String = sqlParts.slice(0, index - 1).mkString(" ")

    // Here we remove the PCATALOG prefix => we replace pcatalog.storagetype.storagesystem.DB.Table with DB.Table
    val createParts = createOnly.split(" ")
    val pcatSQL = createParts.map(element => {
      if (element.toLowerCase().contains(GimelConstants.PCATALOG_STRING)
        || element.toLowerCase().contains(GimelConstants.UDC_STRING) ) {
        // we replace pcatalog.storagetype.storagesystem.DB.Table with DB.Table
        element.split('.').tail.mkString(".").split('.').tail.mkString(".").split('.').tail.mkString(".")
      }
      else {
        element
      }
    }
    ).mkString(" ")

    val sparkSchema = dataSetProps(GimelConstants.TABLE_FILEDS).asInstanceOf[Array[StructField]]
    // From the dataframe schema, translate them into Teradata data types
    val gimelSchema: Array[com.paypal.gimel.common.catalog.Field] = sparkSchema.map(x => {
      com.paypal.gimel.common.catalog.Field(x.name, SparkToJavaConverter.getTeradataDataType(x.dataType), x.nullable)
    })
    val colList: Array[String] = gimelSchema.map(x => (x.fieldName + " " + (x.fieldType) + ","))
    val conCatColumns = colList.mkString("").dropRight(1)
    val colQulifier = s"""(${conCatColumns})"""

    // Here we inject back the columns with data types back in the SQL statemnt
    val newSqlParts = pcatSQL.split(" ")
    val PCATindex = newSqlParts.indexWhere(_.toUpperCase().contains("TABLE"))
    val catPrefix = newSqlParts.slice(0, PCATindex + 2).mkString(" ")
    val catSuffix = newSqlParts.slice(PCATindex + 2, newSqlParts.length).mkString(" ")
    val fullStatement = s"""${catPrefix} ${colQulifier} ${catSuffix}"""
    fullStatement
  }

  /** This method drops the table from teradata database
    *
    * @param dataset      dataset name
    * @param dataSetProps dataset properties
    * @return Boolean
    */
  def drop(dataset: String, dataSetProps: Map[String, Any]): Boolean = {
    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)
    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    val actualProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val dropTableStatement = s"""DROP TABLE ${jdbcOptions(JdbcConfigs.jdbcDbTable)}"""
    val con = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
    val columnStatement: PreparedStatement = con.prepareStatement(dropTableStatement)
    columnStatement.execute()
  }

  /** This method purges data in the table
    *
    * @param dataset      dataset name
    * @param dataSetProps dataset properties
    * @return Boolean
    */
  def truncate(dataset: String, dataSetProps: Map[String, Any]): Boolean = {
    val jdbcConnectionUtility: JDBCConnectionUtility = JDBCConnectionUtility(sparkSession, dataSetProps)
    val jdbcOptions: Map[String, String] = JdbcAuxiliaryUtilities.getJDBCOptions(dataSetProps)
    val actualProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val dropTableStatement = s"""DELETE FROM ${jdbcOptions(JdbcConfigs.jdbcDbTable)}"""
    val con = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand()
    val columnStatement: PreparedStatement = con.prepareStatement(dropTableStatement)
    columnStatement.execute()
  }

}

/**
  * case class to define column
  *
  * @param columName
  * @param columnType
  */
case class Column(columName: String, columnType: String)
