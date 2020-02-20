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

import java.math.RoundingMode
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

import com.google.common.math.{IntMath, LongMath}
import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

import com.paypal.gimel.common.catalog.{DataSetProperties, Field}
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.conf.GimelConstants.ConfidenceIdentifier
import com.paypal.gimel.common.utilities.{DataSetUtils, GenericUtils, SQLDataTypesUtils}
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.utilities.JDBCUtilities.getOrCreateConnection
import com.paypal.gimel.jdbc.utilities.PartitionUtils.ConnectionDetails
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.parser.utilities.{QueryConstants, QueryParserUtils}

object JdbcAuxiliaryUtilities {

  private val logger: Logger = Logger(this.getClass.getName)
  import GenericUtils.{log, logError}
  import JDBCConnectionUtility.withResources

  /**
    *
    * @param resultSet
    * @param columnIndex
    * @return
    */
  def getStringColumn(resultSet: ResultSet, columnIndex: Int): Option[String] = {
    if (resultSet.next()) {
      Some(resultSet.getString(columnIndex))
    } else {
      None
    }
  }

  /**
    * This method gets the Primary key column of Teradata which is any numeric field
    *
    * @param databaseName
    * @param tableName
    * @param dbConn
    * @param NUMERIC_ONLY_FLAG
    * @return
    */
  def getPrimaryKeysForTeradata(databaseName: String, tableName: String, dbConn: Connection,
                                NUMERIC_ONLY_FLAG: Boolean): List[String] = {
    val primaryKeyStatement: String =
      s"""
         |SELECT ColumnName FROM
         |dbc.columnsv
         |WHERE
         |DatabaseName = '${databaseName}'
         |AND TableName = '${tableName}'
         |AND ColumnName IN
         |(SELECT ColumnName FROM dbc.indicesv
         |WHERE DatabaseName = '${databaseName}'
         |AND TableName = '${tableName}'
         |AND IndexType = 'P')
         |
       """.stripMargin

    val updatedPrimaryKeyStatement = if (NUMERIC_ONLY_FLAG) {
      s"$primaryKeyStatement AND ColumnType in ('I','I1','I2','I8', 'F', 'D', 'BF', 'BV') "
    } else {
      primaryKeyStatement
    }

    executeQueryAndGetStringColumn(dbConn, updatedPrimaryKeyStatement)
  }

  /**
    *
    * @param databaseName
    * @param tableName
    * @param dbConn
    * @param NUMERIC_ONLY_FLAG
    * @return
    */
  def getPrimaryKeysForMysql(databaseName: String, tableName: String, dbConn: Connection,
                             NUMERIC_ONLY_FLAG: Boolean): List[String] = {
    val primaryKeyStatement: String =
      s"""
         |SELECT COLUMN_NAME as ColumnName
         |FROM information_schema.COLUMNS
         |WHERE (TABLE_SCHEMA = '${databaseName}')
         |AND (TABLE_NAME = '${tableName}')
         |AND (COLUMN_KEY = 'PRI')
         |
       """.stripMargin

    val updatedPrimaryKeyStatement = if (NUMERIC_ONLY_FLAG) {
      s""" $primaryKeyStatement AND DATA_TYPE in ('INT', 'SMALLINT', 'TINYINT',
         | 'MEDIUMINT', 'BIGINT', 'FLOAT', 'DOUBLE')""".stripMargin
    } else {
      primaryKeyStatement
    }
    executeQueryAndGetStringColumn(dbConn, updatedPrimaryKeyStatement)
  }

  private def executeQueryAndGetStringColumn(dbConn: Connection, updatedPrimaryKeyStatement: String) = {
    withResources(dbConn.prepareStatement(updatedPrimaryKeyStatement)) {
      preparedStatement =>
        withResources(preparedStatement.executeQuery()) {
          resultSet =>
            Iterator
              .continually(getStringColumn(resultSet, columnIndex = 1))
              .takeWhile(_.nonEmpty)
              .flatten
              .toList
        }
    }
  }

  /**
    *
    * @param databaseName
    * @param tableName
    * @param dbConn
    * @param NUMERIC_ONLY_FLAG
    * @return
    */
  def getPrimaryKeysForOracle(databaseName: String, tableName: String, dbConn: Connection,
                              NUMERIC_ONLY_FLAG: Boolean): List[String] = {
    val primaryKeyStatement: String =
      s"""
         |SELECT cols.table_name as ColumnName
         |FROM all_constraints cons, all_cons_columns cols
         |WHERE cols.table_name = '${tableName.toUpperCase}'
         |AND cons.constraint_type = 'P'
         |AND cons.constraint_name = cols.constraint_name
         |AND cons.owner = cols.owner
         |ORDER BY cols.table_name, cols.position;
         |
       """.stripMargin

    //    val updatedPrimaryKeyStatement = if (NUMERIC_ONLY_FLAG) {
    //      primaryKeyStatement
    //    } else {
    //      primaryKeyStatement
    //    }

    executeQueryAndGetStringColumn(dbConn, primaryKeyStatement)
  }


  /**
    * This method returns the primary keys of the table
    *
    * @param url
    * @param dbTable
    * @param dbConn
    * @param NUMERIC_ONLY_FLAG this specifies if only numeric primary keys are need to be retrived
    * @return
    */
  def getPrimaryKeys(url: String, dbTable: String, dbConn: Connection,
                     NUMERIC_ONLY_FLAG: Boolean = false): List[String] = {
    val Array(databaseName, tableName) = dbTable.split("""\.""")

    val jdbcSystem = getJDBCSystem(url)
    jdbcSystem match {
      case JdbcConstants.TERADATA =>
        getPrimaryKeysForTeradata(databaseName, tableName, dbConn, NUMERIC_ONLY_FLAG)

      case JdbcConstants.MYSQL =>
        getPrimaryKeysForMysql(databaseName, tableName, dbConn, NUMERIC_ONLY_FLAG)

      case JdbcConstants.POSTGRESQL =>
        getPrimaryKeysForMysql(databaseName, tableName, dbConn, NUMERIC_ONLY_FLAG)

      case JdbcConstants.ORCALE =>
        getPrimaryKeysForOracle(databaseName, tableName, dbConn, NUMERIC_ONLY_FLAG)

      case _ =>
        List()
    }
  }


  //  /** this method gets the list of columns
  //    *
  //    * @param dataSetProps dataset properties
  //    * @return Boolean
  //    */
  //  def getTableSchema(dataSetProps: Map[String, Any]): List[Column] = {
  //
  //    val jdbcOptions: Map[String, String] = getJDBCOptions(dataSetProps)
  //    val con = JDBCCommons.getJdbcConnection(jdbcOptions("url"), jdbcOptions("user"), jdbcOptions("password"))
  //    val dbTable = jdbcOptions("dbtable")
  //    val Array(databaseName, tableName) = dbTable.split("""\.""")
  //    val getColumnTypesQuery = s"${getColumnsForDBQuery(databaseName)} AND tablename = '${tableName}'"
  //    val columnStatement: PreparedStatement = con.prepareStatement(getColumnTypesQuery)
  //    val columnsResultSet: ResultSet = columnStatement.executeQuery()
  //    var columns: List[Column] = List()
  //    while (columnsResultSet.asInstanceOf[ResultSet].next()) {
  //      val columnName: String = columnsResultSet.asInstanceOf[ResultSet].getString("ColumnName").trim
  //      var columnType: String = columnsResultSet.asInstanceOf[ResultSet].getString("ColumnDataType")
  //      // if columnType s is NULL
  //      if (columnType == null) {
  //        columnType = ""
  //      }
  //      columns = columns :+ Column(columnName, columnType)
  //    }
  //    con.close()
  //    columns
  //  }


  /** this method prepares the query to get the columns
    *
    * @param db database properties
    * @return Boolean
    */
  def getColumnsForDBQuery(db: String): String = {
    // get the columns for  particular table
    val getColumnTypesQuery: String =
      s"""
         | SELECT DatabaseName, TableName, ColumnName, CASE ColumnType
         |    WHEN 'BF' THEN 'BYTE('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
         |    WHEN 'BV' THEN 'VARBYTE('         || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
         |    WHEN 'CF' THEN 'CHAR('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
         |    WHEN 'CV' THEN 'VARCHAR('         || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
         |    WHEN 'D' THEN 'DECIMAL('         || TRIM(DecimalTotalDigits) || ','
         |                                      || TRIM(DecimalFractionalDigits) || ')'
         |    WHEN 'DA' THEN 'DATE'
         |    WHEN 'F ' THEN 'FLOAT'
         |    WHEN 'I1' THEN 'BYTEINT'
         |    WHEN 'I2' THEN 'SMALLINT'
         |    WHEN 'I8' THEN 'BIGINT'
         |    WHEN 'I ' THEN 'INTEGER'
         |    WHEN 'AT' THEN 'TIME('            || TRIM(DecimalFractionalDigits) || ')'
         |    WHEN 'TS' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits) || ')'
         |    WHEN 'TZ' THEN 'TIME('            || TRIM(DecimalFractionalDigits) || ')' || ' WITH TIME ZONE'
         |    WHEN 'SZ' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits) || ')' || ' WITH TIME ZONE'
         |    WHEN 'YR' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits) || ')'
         |    WHEN 'YM' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits) || ')'      || ' TO MONTH'
         |    WHEN 'MO' THEN 'INTERVAL MONTH('  || TRIM(DecimalTotalDigits) || ')'
         |    WHEN 'DY' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'
         |    WHEN 'DH' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'      || ' TO HOUR'
         |    WHEN 'DM' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'      || ' TO MINUTE'
         |    WHEN 'DS' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits) || ')'      || ' TO SECOND('
         |                                      || TRIM(DecimalFractionalDigits) || ')'
         |    WHEN 'HR' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits) || ')'
         |    WHEN 'HM' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits) || ')'      || ' TO MINUTE'
         |    WHEN 'HS' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits) || ')'      || ' TO SECOND('
         |                                      || TRIM(DecimalFractionalDigits) || ')'
         |    WHEN 'MI' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits) || ')'
         |    WHEN 'MS' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits) || ')'      || ' TO SECOND('
         |                                      || TRIM(DecimalFractionalDigits) || ')'
         |    WHEN 'SC' THEN 'INTERVAL SECOND(' || TRIM(DecimalTotalDigits) || ','
         |                                      || TRIM(DecimalFractionalDigits) || ')'
         |    WHEN 'BO' THEN 'BLOB('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
         |    WHEN 'CO' THEN 'CLOB('            || TRIM(CAST(ColumnLength AS INTEGER)) || ')'
         |
         |    WHEN 'PD' THEN 'PERIOD(DATE)'
         |    WHEN 'PM' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits) || ')' || ' WITH TIME ZONE'
         |    WHEN 'PS' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits) || '))'
         |    WHEN 'PT' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits) || '))'
         |    WHEN 'PZ' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits) || '))' || ' WITH TIME ZONE'
         |    WHEN 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)
         |
         |    WHEN '++' THEN 'TD_ANYTYPE'
         |    WHEN 'N'  THEN 'NUMBER('          || CASE WHEN DecimalTotalDigits = -128 THEN '*' ELSE TRIM(DecimalTotalDigits) END
         |                                      || CASE WHEN DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || TRIM(DecimalFractionalDigits) END
         |                                      || ')'
         |    WHEN 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
         |    WHEN 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
         |
         |    ELSE '<Unknown> ' || ColumnType
         |  END
         |  || CASE
         |        WHEN ColumnType IN ('CV', 'CF', 'CO')
         |        THEN CASE CharType
         |                WHEN 1 THEN ' CHARACTER SET LATIN'
         |                WHEN 2 THEN ' CHARACTER SET UNICODE'
         |                WHEN 3 THEN ' CHARACTER SET KANJISJIS'
         |                WHEN 4 THEN ' CHARACTER SET GRAPHIC'
         |                WHEN 5 THEN ' CHARACTER SET KANJI1'
         |                ELSE ''
         |             END
         |         ELSE ''
         |      END AS ColumnDataType
         |
         |      from dbc.COLUMNSV where databasename='${db}'
               """.stripMargin

    getColumnTypesQuery
  }

  /**
    * This method truncates the given table give the connection to JDBC data source
    *
    * @param url     URL of JDBC system
    * @param dbTable table name
    * @param dbconn  JDBC data source connection
    * @return Boolean
    */
  def truncateTable(url: String, dbTable: String, dbconn: Connection,
                    logger: Option[Logger] = None): Boolean = {
    val jdbcSystem = getJDBCSystem(url)
    jdbcSystem match {
      case JdbcConstants.MYSQL =>
        val truncateTableStatement = s"DELETE FROM $dbTable"
        if (logger.isDefined) {
          logger.get.info(s"In TRUNCATE: $truncateTableStatement")
        }
        executeQueryStatement(truncateTableStatement, dbconn)
      case JdbcConstants.TERADATA =>
        val truncateTableStatement = s"DELETE $dbTable ALL"
        if (logger.isDefined) {
          logger.get.info(s"In TRUNCATE: $truncateTableStatement")
        }
        executeQueryStatement(truncateTableStatement, dbconn)
      case _ =>
        throw new Exception(s"This JDBC System is not supported. Please check gimel docs.")
    }
  }

  /**
    * This method validates if the incoming table is available
    *
    * @param tableName
    * @param jdbcConnectionUtility
    * @param logger
    * @return
    */
  def isTargetTableAvailable(tableName: String,
                             jdbcConnectionUtility: JDBCConnectionUtility,
                             logger: Option[Logger] = None): Boolean = {

    Try {
      JdbcAuxiliaryUtilities.isPushDownEnabled(transformedSQL = s"select * from $tableName",
        jdbcConnectionUtility,
        logger)
    } match {
      case Success(_) =>
        logger.get.info(s"Target table: $tableName exists")
        true
      case Failure(exception) =>
        logger.get.info(s"Target table: $tableName doesn't exist and failed " +
          s"with the exception: ${exception.getMessage}")
        throw new IllegalStateException(s"Cannot proceed with JDBC write as Table: '$tableName' doesn't exist")
    }
  }

  /**
    *
    * @param dataFrame
    * @return
    */
  def getFieldsFromDataFrame(dataFrame: DataFrame): Array[Field] = {

    // construct a schema for Teradata table
    val schema: StructType = dataFrame.schema
    val fields: Array[Field] = schema.map { x =>
      Field(x.name,
        SparkToJavaConverter.getTeradataDataType(x.dataType),
        x.nullable)
    }.toArray
    fields
  }


  /**
    * This method generates the UPADTE statement dynamically
    *
    * @param dbtable      tablename of the JDBC data source
    * @param setColumns   Array of dataframe columns
    * @param whereColumns Array of dataframe columns
    * @return updateStatement
    *         Update statement as a String
    */
  def getUpdateStatement(dbtable: String, setColumns: List[String], whereColumns: List[String]): String = {
    require(setColumns.nonEmpty, s"Column names cannot be an empty array for table $dbtable.")
    require(setColumns.nonEmpty, s"The set of primary keys cannot be empty for table $dbtable.")
    val setColumnsStatement = setColumns.map(columnName => s"${columnName} = ?")
    val separator = ", "
    val statementStart = s"UPDATE $dbtable SET "
    val whereConditions = whereColumns.map { key => s"${key} = ?" }
    val whereClause = whereConditions.mkString(" WHERE ", " AND ", "")
    setColumnsStatement.mkString(statementStart, separator, whereClause)
  }

  /**
    * prepareCreateStatement - From the column details passed in datasetproperties from GimelQueryProcesser,
    * create statement is constructed. If user passes primary index , set or multi set table, those will be added in the create statement
    *
    * @param dbtable      - Table Name
    * @param dataSetProps - Data set properties
    * @return - the created prepared statement for creating the table
    */
  def prepareCreateStatement(dbtable: String, dataSetProps: Map[String, Any]): String = {
    val sparkSchema = dataSetProps(GimelConstants.TABLE_FILEDS).asInstanceOf[Array[StructField]]
    // From the dataframe schema, translate them into Teradata data types
    val gimelSchema: Array[com.paypal.gimel.common.catalog.Field] = sparkSchema.map(x => {
      com.paypal.gimel.common.catalog.Field(x.name, SparkToJavaConverter.getTeradataDataType(x.dataType), x.nullable)
    })
    val colList: Array[String] = gimelSchema.map(x => (x.fieldName + " " + (x.fieldType) + ","))
    val conCatColumns = colList.mkString("")
    val paramsMapBuilder = dataSetProps(GimelConstants.TBL_PROPERTIES).asInstanceOf[Map[String, String]]
    val (tableType: String, indexString: String) = paramsMapBuilder.size match {
      case 0 => ("", "")
      case _ =>
        // If table type, primary index are passed, use them in the create statement
        val tableType = paramsMapBuilder.getOrElse(GimelConstants.TERA_DATA_TABLE_TYPE_COLUMN, "")
        val indexName = paramsMapBuilder.getOrElse(GimelConstants.TERA_DATA_INDEX_NAME_COLUMN, "")
        val indexColumn = paramsMapBuilder.getOrElse(GimelConstants.TERA_DATA_INDEX_COLUMN, "")
        paramsMapBuilder.contains("index_name") match {
          case true => (tableType, s"""${indexName}(${indexColumn})""")
          case _ => (tableType, "")
        }
    }
    s"""CREATE ${tableType} TABLE ${dbtable} (${conCatColumns.dropRight(1)} ) ${indexString}  """
  }

  /**
    * This method generates the INSERT statement dynamically
    *
    * @param dbtable tablename of the JDBC data source
    * @param cols    Number of columns in the table
    * @return insertStatement
    *         Insert statement as a String
    */
  def getInsertStatement(dbtable: String, cols: Int): String = {
    val statementStart = s"INSERT INTO $dbtable  VALUES( "
    val statementEnd = ")"
    val separator = ", "

    Seq.fill(cols)("?").mkString(statementStart, separator, statementEnd)
  }


  /**
    *
    * @param column column to get min-max for
    * @param conn   connection parameter
    * @return Tuple of min and max value of that column
    */
  def getMinMax(column: String, dbTable: String, conn: Connection): (Double, Double) = {
    val getMinMaxStatement = s"""SELECT MIN($column) as lowerBound, MAX($column) as upperBound FROM $dbTable"""
    val columnStatement: PreparedStatement = conn.prepareStatement(getMinMaxStatement)
    try {
      val resultSet: ResultSet = columnStatement.executeQuery()
      if (resultSet.next()) {
        val lowerBound = resultSet.getObject("lowerbound")
        val upperBound = resultSet.getObject("upperbound")
        if (lowerBound != null && upperBound != null) {
          (lowerBound.toString.toDouble, upperBound.toString.toDouble)
        }
        else {
          (0.0, 20.0)
        }
      }
      else {
        (0.0, 20.0)
      }
    }
    catch {
      case e: Exception =>
        logError(s"Error while getting min & max value from $dbTable: ${e.getMessage}", e, logger)
        throw new IllegalStateException(s"Error while getting min & max value from $dbTable: ${e.getMessage}", e)
    }
  }

  /**
    *
    * @param dbtable
    * @param con
    */
  def dropTable(dbtable: String, con: Connection, digestException: Boolean = true,
                logger: Option[Logger] = None): Unit = {
    val dropStatement =
      s"""
         | DROP TABLE $dbtable
       """.stripMargin
    if (logger.isDefined) {
      logger.get.info(s"About to execute DROP -> $dropStatement")
    }
    try {
      executeQueryStatement(dropStatement, con)
    }
    catch {
      case e: Exception =>
        // if the table doesn't exist then ignoring the exception
        new Logger(this.getClass.getName).error("Digesting drop table exception! ", e)
        if (!digestException) {
          throw e
        }
    }
  }


  /**
    *
    * @param dbTable       table
    * @param con           connection
    * @param numPartitions num of partitions
    * @return
    */
  def insertPartitionsIntoTargetTable(dbTable: String, con: Connection, numPartitions: Int,
                                      logger: Option[Logger] = None): Boolean = {
    val insertStatement =
      s"""
         | INSERT INTO $dbTable
         | ${getUnionAllStatetementFromPartitions(dbTable, numPartitions)}
       """.stripMargin
    if (logger.isDefined) {
      logger.get.info(s"About to execute INSERT -> $insertStatement")
    }
    executeQueryStatement(insertStatement, con)
  }

  /**
    *
    * @param dbTable       table
    * @param numPartitions num of partitions
    * @return
    */
  def getUnionAllStatetementFromPartitions(dbTable: String, numPartitions: Int): String = {
    val selStmt =
      """
        | SELECT * FROM
      """.stripMargin
    (0 until numPartitions).map(partitionId =>
      s"$selStmt ${dbTable}_${JdbcConstants.GIMEL_TEMP_PARTITION}_$partitionId"
    ).mkString(" UNION ALL ")
  }

  /**
    *
    * @param dbtable
    * @param con
    * @param numPartitions
    */
  def dropAllPartitionTables(dbtable: String, con: Connection, numPartitions: Int,
                             logger: Option[Logger] = None): Unit = {
    for (partitionId <- 0 until numPartitions) {
      JdbcAuxiliaryUtilities.dropTable(s"${dbtable}_${JdbcConstants.GIMEL_TEMP_PARTITION}_$partitionId",
        con, digestException = false)
    }
  }

  /**
    *
    * @param jdbcConnectionUtility
    * @param jdbcHolder
    * @return
    */
  def createConnectionWithPreConfigsSet(jdbcConnectionUtility: JDBCConnectionUtility,
                                        jdbcHolder: JDBCArgsHolder,
                                        connection: Option[Connection] = None,
                                        logger: Option[Logger] = None): Connection = {
    if (connection.isDefined && !connection.get.isClosed) {
      connection.get
    } else {
      val dbc = jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand(logger)
      JdbcAuxiliaryUtilities.executePreConfigs(jdbcHolder.jdbcURL, jdbcHolder.dbTable, dbc)
      dbc
    }
  }

  /**
    *
    * @param query string
    * @param con   jdbc connection
    * @return
    */
  def executeQueryStatement(query: String, con: Connection,
                            validateStatementExecution: Boolean = false,
                            incomingLogger: Option[Logger] = None,
                            recordTimeTakenToExecute: Boolean = false): Boolean = {
    val isStatementExecutionSuccess = JDBCConnectionUtility.withResources(con.prepareStatement(query)) { statement =>
      val (executeStatus: Boolean, elapsedTime: Long) = time(statement.execute())
      if(!con.getAutoCommit) {
        con.commit()
      }
      if (recordTimeTakenToExecute) {
        val message = s"Time taken " +
          s"[${DurationFormatUtils.formatDurationWords(elapsedTime, true, true)}]" +
          s"  to execute $query"
        if (incomingLogger.isDefined) {
          incomingLogger.get.info(message)
        } else {
          logger.info(message)
        }
      }
      executeStatus
    }
    if (validateStatementExecution && !isStatementExecutionSuccess) {
      throw new IllegalStateException(s"Failed to execute query : $query")
    }
    isStatementExecutionSuccess
  }


  /**
    * Function to determine the elapsed time taken for function execution and return it,
    *
    * @param block -> function block to be executed
    * @tparam R -> function return type
    * @return R, Long: elapsed time for the code block
    */
  def time[R](block: => R): (R, Long) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()

    val elapsedTime = (t1 - t0) / 1e6

    (result, elapsedTime.toLong)
  }

  /**
    *
    * @param dbTable jdbc tablename
    * @param con     jdbc connection
    * @return ddl fo the table
    */
  def getDDL(url: String, dbTable: String, con: Connection): String = {

    val jdbcSystem = getJDBCSystem(url)

    // get the DDL for table for jdbc system
    import JDBCConnectionUtility.withResources
    val ddl = jdbcSystem match {
      case JdbcConstants.TERADATA =>
        val showTableQuery = s"SHOW TABLE $dbTable"
        executeQuery(con, showTableQuery)
      case JdbcConstants.MYSQL =>
        val showTableQuery = s"SHOW CREATE TABLE $dbTable"
        withResources(con.prepareStatement(showTableQuery)) {
          preparedStatement =>
            withResources(preparedStatement.executeQuery()) {
              resultSet =>
                val stringBuilder = new StringBuilder
                while (resultSet.next()) {
                  stringBuilder ++= resultSet.getString("Create Table")
                    .replaceAll("[\u0000-\u001f]", " ").replaceAll(" +", " ")
                }
                val tempDdl = stringBuilder.toString
                // if ddl does not contain the database name, replace table name with db.tablename
                val ddlString = if (!tempDdl.contains(dbTable)) {
                  val tableName = dbTable.split("\\.")(1)
                  tempDdl.replace(tableName, dbTable)
                }
                else {
                  tempDdl
                }
                ddlString
            }
        }
      case _ =>
        throw new Exception(s"The JDBC System for $url is not supported")
    }
    ddl
  }

  /**
    * Utility for executing select query statement and get the return result as constructed lines from the resultset
    *
    * @param con       -> JDBC connection
    * @param query     -> Query to be executed
    * @param delimiter -> Query to be executed
    * @return
    */
  def executeQuery(con: Connection, query: String,
                   delimiter: String = GimelConstants.EMPTY_STRING,
                   isUpdatable: Boolean = false): String = {
    import JDBCConnectionUtility.withResources
    withResources(
      if (isUpdatable) {
        con.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)
      } else {
        con.prepareStatement(query)
      }) {
      preparedStatement =>
        withResources(preparedStatement.executeQuery()) {
          resultSet =>
            val stringBuilder = new StringBuilder
            while (resultSet.next()) {
              stringBuilder.append(resultSet.getString(1)
                .replaceAll("[\u0000-\u001f]", " ")
                .replaceAll(" +", " "))
              stringBuilder.append(delimiter)
            }
            stringBuilder.toString()
        }
    }
  }

  /**
    * Utility for executing update statement and get the return result as constructed lines from the resultset
    *
    * @param con       -> JDBC connection
    * @param query     -> Query to be executed
    * @param delimiter -> Query to be executed
    * @return
    */
  def executeUpdate(con: Connection, query: String, delimiter: String = GimelConstants.EMPTY_STRING): String = {
    import JDBCConnectionUtility.withResources
    withResources(con.prepareStatement(query)) {
      preparedStatement => preparedStatement.executeUpdate().toString
    }
  }

  /**
    * This returns the dataset properties based on the attribute set in the properties map
    *
    * @param dataSetProps
    * @return
    */
  def getDataSetProperties(dataSetProps: Map[String, Any]): Map[String, Any] = {
    val dsetProperties: Map[String, Any] = if (dataSetProps.contains(GimelConstants.DATASET_PROPS)) {
      val datasetPropsOption: DataSetProperties =
        dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
      datasetPropsOption.props ++ dataSetProps
    } else {
      dataSetProps
    }
    dsetProperties
  }

  /**
    *
    * @param dsetProperties
    * @return
    */
  def getMysqlOptions(dsetProperties: Map[String, Any]): Map[String, String] = {

    // url
    val url = getJdbcUrl(dsetProperties)
    // driver
    val driver = dsetProperties(JdbcConfigs.jdbcDriverClassKey).toString
    val jdbcOptions: Map[String, String] = Map(JdbcConfigs.jdbcUrl -> url, JdbcConfigs.jdbcDriverClassKey -> driver)
    jdbcOptions
  }

  /**
    *
    * @param dsetProperties
    * @return
    */
  def getTeradataOptions(dsetProperties: Map[String, Any]): Map[String, String] = {

    // url
    val url = getJdbcUrl(dsetProperties)
    // driver
    val driver = dsetProperties(JdbcConfigs.jdbcDriverClassKey).toString

    val jdbcOptions: Map[String, String] = Map(JdbcConfigs.jdbcUrl -> url, JdbcConfigs.jdbcDriverClassKey -> driver)
    jdbcOptions
  }

  /**
    * This method will retrieve JDBC system specific options
    *
    * @param dsetProperties
    * @return
    */
  def getJdbcStorageOptions(dsetProperties: Map[String, Any]): Map[String, String] = {

    // url
    val url = dsetProperties(JdbcConfigs.jdbcUrl).toString

    val jdbcSystem = getJDBCSystem(url)
    val jdbcOptions: Map[String, String] = jdbcSystem match {
      case JdbcConstants.TERADATA =>
        getTeradataOptions(dsetProperties)
      case _ =>
        getMysqlOptions(dsetProperties)
    }
    jdbcOptions
  }

  /**
    * This function returns the JDBC properties map from HIVE table properties
    *
    * @param dataSetProps This will set all thew options required for READ/WRITE from/to JDBC data source
    * @return jdbcOptions
    */
  def getJDBCOptions(dataSetProps: Map[String, Any]): Map[String, String] = {
    val dsetProperties: Map[String, Any] = getDataSetProperties(dataSetProps)

    val jdbcOptions = getJdbcStorageOptions(dsetProperties)

    // table
    if (dsetProperties.contains(JdbcConfigs.jdbcInputTableNameKey)) {
      val jdbcTableName = dsetProperties(JdbcConfigs.jdbcInputTableNameKey).toString
      jdbcOptions + (JdbcConfigs.jdbcDbTable -> jdbcTableName)
    } else {
      jdbcOptions
    }
  }

  /**
    * This function returns the JDBC properties map from HIVE table properties
    *
    * @param dataSetProps      This will set all thew options required for READ/WRITE from/to JDBC data source
    * @param systemJdbcOptions Incoming JDBC options coming from catalog provider
    * @return jdbcOptions
    */
  def mergeJDBCOptions(dataSetProps: Map[String, Any], systemJdbcOptions: Map[String, String]): Map[String, String] = {
    val datasetProperties: Map[String, Any] = getDataSetProperties(dataSetProps)
    logger.info(s"MERGED dataset properties: $datasetProperties")
    (systemJdbcOptions ++ datasetProperties).flatMap {
      case (k, v) if k.toLowerCase.startsWith(GimelConstants.GIMEL_JDBC_OPTION_KEYWORD) &&
        k.equalsIgnoreCase(JdbcConfigs.jdbcInputTableNameKey) => Some(JdbcConfigs.jdbcDbTable -> v.toString)
      case (k, _) if systemJdbcOptions.contains(k) => Some(k -> systemJdbcOptions(k).toString)
      case (k, v) if k.toLowerCase.startsWith(GimelConstants.GIMEL_JDBC_OPTION_KEYWORD) => Some(k -> v.toString)
      case _ => None
    }
  }

  /**
    * Utility for building JDBC system specific connection URL
    *
    * @param dataSetProps -> Connection properties received
    * @return string url
    */
  def getJdbcUrl(dataSetProps: Map[String, Any],
                 connectionDetails: Option[PartitionUtils.ConnectionDetails] = None): String = {
    val dsetProperties: Map[String, Any] = getDataSetProperties(dataSetProps)
    // get basic url
    val url = dsetProperties(JdbcConfigs.jdbcUrl).toString

    val jdbcSystem = getJDBCSystem(url)
    val jdbcUrl = jdbcSystem match {
      case JdbcConstants.TERADATA =>
        val charset = GenericUtils.getConfigValueFromCaseInsensitiveKey(GimelConstants.JDBC_CHARSET_KEY,
          dataSetProps, JdbcConstants.DEFAULT_CHARSET)
        val teradataReadType: String = dataSetProps.getOrElse(JdbcConfigs.teradataReadType, "").toString
        val teradataWriteType: String = dataSetProps.getOrElse(JdbcConfigs.teradataWriteType, "").toString

        // get sessions
        val teradataSessions: String = dataSetProps.getOrElse("SESSIONS", JdbcConstants.DEFAULT_SESSIONS).toString
        val urlBuilder = new StringBuilder(url.toLowerCase())

        if (urlBuilder.indexOf("charset") == -1) {
          urlBuilder.append(s"/charset=$charset")
        }
        // checking lower case as per the stringbuilder above
        if (teradataReadType.toUpperCase.equals(JdbcConstants.TD_FASTEXPORT_KEY)
          && urlBuilder.indexOf(s"type=${JdbcConstants.TD_FASTEXPORT_KEY_LC}") == -1) {
          // Teradata READ type
          urlBuilder.append(GimelConstants.COMMA).append(s"TYPE=${JdbcConstants.TD_FASTEXPORT_KEY}")
        } else if (teradataWriteType.toUpperCase.equals(JdbcConstants.TD_FASTLOAD_KEY)
          && urlBuilder.indexOf(s"type=${JdbcConstants.TD_FASTLOAD_KEY_LC}") == -1) {
          // Teradata Write type
          urlBuilder.append(GimelConstants.COMMA).append(s"TYPE=${JdbcConstants.TD_FASTLOAD_KEY}")
        }
        if (urlBuilder.indexOf("sessions=") == -1) {
          urlBuilder.append(GimelConstants.COMMA).append(s"SESSIONS=$teradataSessions")
        }
        if (connectionDetails.isDefined && connectionDetails.get.errLimit.isDefined) {
          urlBuilder.append(GimelConstants.COMMA).append(s"ERRLIMIT=${connectionDetails.get.errLimit.get}")
        }
        urlBuilder.toString()
      case _ =>
        url
    }
    jdbcUrl
  }

  /**
    * These are some pre configs to set as soon as the connection is created
    *
    * @param url
    * @param dbTable
    * @param con
    * @return
    */
  def executePreConfigs(url: String, dbTable: String, con: Connection): Unit = {

    val jdbcSystem = getJDBCSystem(url)
    jdbcSystem match {
      case JdbcConstants.MYSQL =>
        // set the database to use for MYSQL
        val db = dbTable.split("\\.")(0)
        executeQueryStatement(s"USE $db", con)
      case _ =>
      // do nothing
    }
  }

  /**
    *
    * @param jdbcUrl
    * @return
    */
  def getJDBCSystem(jdbcUrl: String): String = {
    if (jdbcUrl.toLowerCase.contains("jdbc:teradata")) {
      JdbcConstants.TERADATA
    }
    else if (jdbcUrl.toLowerCase.contains("jdbc:mysql")) {
      JdbcConstants.MYSQL
    }
    else if (jdbcUrl.toLowerCase.contains("jdbc:oracle")) {
      JdbcConstants.ORCALE
    }
    else if (jdbcUrl.toLowerCase.contains("jdbc:postgresql")) {
      JdbcConstants.POSTGRESQL
    }
    else {
      throw new Exception(s"This JDBC System with ${jdbcUrl} is not supported. Please check gimel docs.")
    }
  }

  /** This method creates the table in teradata database
    *
    * @param dbTable JDBC table name in format db.table
    * @param fields  schama fields in dataframe
    * @return Boolean
    */
  def createJDBCTable(dbTable: String, fields: Array[Field], con: Connection): Boolean = {
    var colList = ""
    fields.foreach(x => colList = colList + x.fieldName + " " + (x.fieldType) + ",")
    val createTableStatement = s"""CREATE TABLE ${dbTable} (${colList.dropRight(1)} ) """
    val columnStatement: PreparedStatement = con.prepareStatement(createTableStatement)
    columnStatement.execute()
  }

  /**
    * This method returns the partition table name based on jdbc system
    *
    * @param url
    * @param dbTable
    * @param con
    * @return
    */
  def getPartitionTableName(url: String, dbTable: String, con: Connection, partitionId: Int): String = {

    val jdbcSystem = getJDBCSystem(url)
    val partitionTable = jdbcSystem match {
      case JdbcConstants.MYSQL =>
        s"${dbTable.split("\\.")(1)}_${JdbcConstants.GIMEL_TEMP_PARTITION}_$partitionId"

      case JdbcConstants.TERADATA =>
        s"${dbTable}_${JdbcConstants.GIMEL_TEMP_PARTITION}_$partitionId"

      case _ =>
        throw new Exception(s"This JDBC System is not supported. Please check gimel docs.")
    }
    partitionTable
  }

  /**
    * This method retuns the partitioned dataframe based on the partition method
    * and the current number of partitions in df
    *
    * @param url
    * @param df
    * @param userSpecifiedPartitions
    * @return
    */
  def getPartitionedDataFrame(url: String, df: DataFrame, userSpecifiedPartitions: Option[Any]): DataFrame = {
    val currentPartitions = df.rdd.getNumPartitions
    val maxAllowedPartitions = getMaxPartitions(url, JdbcConstants.WRITE_OPERATION)
    val (partitions, partitionMethod) = userSpecifiedPartitions match {
      case None =>
        (Math.min(maxAllowedPartitions, currentPartitions), JdbcConstants.COALESCE_METHOD)
      case _ =>
        val userPartitions = Try(
          userSpecifiedPartitions.get.toString.toInt
        ).getOrElse(JdbcConstants.DEFAULT_JDBC_WRTIE_PARTITIONS)
        if (userPartitions > currentPartitions) {
          (Math.min(userPartitions, maxAllowedPartitions), JdbcConstants.REPARTITION_METHOD)
        } else {
          (Math.min(userPartitions, maxAllowedPartitions), JdbcConstants.COALESCE_METHOD)
        }
    }

    // this case is added specifically for the dataframe having no data i.e. partitions = 0
    val partitionsToSet = Math.max(partitions, 1)

    logger.info(s"Setting number of partitions of the dataFrame to ${partitionsToSet} with ${partitionMethod}")
    val partitionedDataFrame = partitionMethod match {
      case JdbcConstants.REPARTITION_METHOD =>
        df.repartition(partitionsToSet)
      case JdbcConstants.COALESCE_METHOD =>
        df.coalesce(partitionsToSet)
      case _ =>
        df
    }
    partitionedDataFrame
  }

  /**
    * This method returns the max number of partitions for the JDBC system based on the operation
    *
    * @param url
    * @param operationMethod
    */
  def getMaxPartitions(url: String, operationMethod: String): Int = {
    val jdbcSystem = getJDBCSystem(url)
    val defaultPartitions = jdbcSystem match {
      case JdbcConstants.TERADATA =>
        operationMethod match {
          case JdbcConstants.READ_OPERATION if url.contains(JdbcConstants.TD_FASTEXPORT_KEY) =>
            JdbcConstants.MAX_FAST_EXPORT_READ_PARTITIONS
          case JdbcConstants.READ_OPERATION =>
            JdbcConstants.MAX_TD_JDBC_READ_PARTITIONS
          case JdbcConstants.WRITE_OPERATION if url.contains(JdbcConstants.TD_FASTLOAD_KEY) =>
            JdbcConstants.MAX_FAST_LOAD_WRITE_PARTITIONS
          case JdbcConstants.WRITE_OPERATION =>
            JdbcConstants.MAX_TD_JDBC_WRITE_PARTITIONS
        }
      case _ =>
        operationMethod match {
          case JdbcConstants.READ_OPERATION =>
            JdbcConstants.DEFAULT_JDBC_READ_PARTITIONS
          case JdbcConstants.WRITE_OPERATION =>
            JdbcConstants.DEFAULT_JDBC_WRTIE_PARTITIONS
        }
    }
    defaultPartitions
  }

  /**
    * This method returns the number of partitions based on user parameter and JDBC system
    *
    * @param url
    * @param userSpecifiedPartitions
    * @param operationMethod
    * @return
    */
  def getNumPartitions(url: String, userSpecifiedPartitions: Option[Any], operationMethod: String): Int = {
    val logger = Logger()
    val jdbcSystem = getJDBCSystem(url)
    val maxAllowedPartitions = getMaxPartitions(url, operationMethod)
    val userPartitions = userSpecifiedPartitions.getOrElse(maxAllowedPartitions).toString.toInt
    if (userPartitions > maxAllowedPartitions) {
      logger.info(s"Warning: Maximum number of partitions are SET to ${maxAllowedPartitions} " +
        s"for ${jdbcSystem} due to connections limitations")
    }
    val numPartitions = Math.min(userPartitions, maxAllowedPartitions)
    logger.info(s"Maximum number of partitions are SET to ${numPartitions} for ${jdbcSystem}")
    numPartitions
  }

  /**
    *
    * @param sparkSession
    * @param url
    * @param table
    * @param partitionColumn
    * @param lowerBound
    * @param upperBound
    * @param numPartitions
    * @param fetchSize
    * @param connectionProperties
    * @return
    */
  def sparkJdbcRead(sparkSession: SparkSession, url: String, table: String, partitionColumn: Option[String] = None,
                    lowerBound: Long, upperBound: Long,
                    numPartitions: Int, fetchSize: Int,
                    connectionProperties: Properties): DataFrame = {

    val logger = Logger(this.getClass.getName)
    logger.info(s"Using the default spark JDBC read for ${url}")
    if (partitionColumn.isDefined) {
      sparkSession.read.jdbc(url = url,
        table = table,
        columnName = partitionColumn.get,
        lowerBound = lowerBound,
        upperBound = upperBound,
        numPartitions = numPartitions,
        connectionProperties = connectionProperties)
    } else {
      sparkSession.read.jdbc(url = url, table = table, properties = connectionProperties)
    }
  }

  def getConnectionDetailsFromExplainPlan(sparkSession: SparkSession,
                                          connection: Connection,
                                          sqlToBeExecuted: String,
                                          logger: Option[Logger] = None,
                                          partitionColumns: Seq[String] = Seq.empty): ConnectionDetails = {
    val response: String = executeExplainPlan(connection, sqlToBeExecuted)
    if (logger.isDefined) {
      logger.get.info(response)
      logger.get.info("Pattern matcher")
    }

    val responseWithNewLineStrippedOff = response.replaceAll(GimelConstants.NEW_LINE, GimelConstants.EMPTY_STRING)
    val rows: Long = parseNumeric(GimelConstants.ROWS_IDENTIFIER, responseWithNewLineStrippedOff, logger).longValue()
    val overallSizeOfRecordsInBytes: Long = parseNumeric(GimelConstants.SPACE_IDENTIFIER,
      responseWithNewLineStrippedOff, logger).longValue()
    // get confidence level
    val confidence = parseConfidence(GimelConstants.CONFIDENCE_IDENTIFIER, responseWithNewLineStrippedOff, logger)

    //    val isjdbcCompletePushdownSelectEnabled: Boolean =
    //      sparkSession.conf.getOption(JdbcConfigs.jdbcCompletePushdownSelectEnabled).getOrElse("false").toBoolean
    val isPartitionColumnsSpecified = partitionColumns.nonEmpty
    val isHavingSampleRows = QueryParserUtils.isHavingSampleRows(responseWithNewLineStrippedOff)
    // Common validations on the incoming request
    if (!isHavingSampleRows) {
      validateMaxRowsToBeProcessed(sparkSession, rows, confidence = confidence)
    }

    // Set ERRLIMIT, if specified
    val userSpecifiedErrorLimit: Option[Int] =
      if (sparkSession.conf.getOption(JdbcConfigs.jdbcFastLoadErrLimit).isDefined) {
        GenericUtils.parseInt(sparkSession.conf.get(JdbcConfigs.jdbcFastLoadErrLimit))
      } else if (sparkSession.conf.getOption(s"spark.${JdbcConfigs.jdbcFastLoadErrLimit}").isDefined) {
        GenericUtils.parseInt(sparkSession.conf.get(s"spark.${JdbcConfigs.jdbcFastLoadErrLimit}"))
      } else {
        None
      }
    if (logger.isDefined) {
      logger.get.info(s"User specified error limit: $userSpecifiedErrorLimit")
    }

    val connectionDetails = computeConnectionDetails(rows = rows, overallSizeOfRecords = overallSizeOfRecordsInBytes,
      explainPlan = responseWithNewLineStrippedOff, logger = logger,
      confidence = confidence, errorLimit = userSpecifiedErrorLimit)
    val computedConnectionDetails = if (connectionDetails.numOfPartitions > 1 &&
      (isHavingSampleRows || !isPartitionColumnsSpecified)) {
      // TODO should confidence level be used along with the above check
      if (logger.isDefined) {
        logger.get.info(s"Computed connection details: $connectionDetails")
        val reason = if (isHavingSampleRows) {
          "as the explain plan is having sample rows in it"
        } else {
          s"as partition columns is not specified: $isPartitionColumnsSpecified, " +
            s"we will be reverting it back to one partition now"
        }
        logger.get.info(s"Overriding the number of partitions from " +
          s"${connectionDetails.numOfPartitions} to 1 and Connection TYPE to JDBC, $reason")
      }
      connectionDetails.copy(numOfPartitions = 1, jdbcUrlType = ConnectionDetails.JDBC_CONNECTION_URL_TYPE)
    } else {
      if (logger.isDefined) {
        logger.get.info(s"Partition columns are specified, User specified: ${
          if (sparkSession.conf.getOption(JdbcConfigs.jdbcPartitionColumns).isDefined) {
            sparkSession.conf.getOption(JdbcConfigs.jdbcPartitionColumns).get
          } else {
            isPartitionColumnsSpecified
          }
        } and received from respective table: $partitionColumns")
      }
      connectionDetails
    }
    computedConnectionDetails
  }

  def executeOracleExplainPlan(connection: Connection,
                               sqlToBeExecuted: String): String = {
    JdbcAuxiliaryUtilities
      .executeQuery(
        connection,
        s"${GimelConstants.ORACLE_EXPLAIN_CONTEXT} ${QueryParserUtils.ltrim(sqlToBeExecuted)}",
        GimelConstants.NEW_LINE
      )
  }

  def executeExplainPlan(connection: Connection,
                         sqlToBeExecuted: String): String = {
    Try {
      JdbcAuxiliaryUtilities
        .executeQuery(
          connection,
          s"${GimelConstants.EXPLAIN_CONTEXT} ${QueryParserUtils.ltrim(sqlToBeExecuted)}",
          GimelConstants.NEW_LINE
        )
    } match {
      case Success(value) => value
      case Failure(exception) =>
        logger.error(s"Unable to execute explain plan query: $sqlToBeExecuted")
        throw exception
    }
  }

  def validatePushDownQuery(sparkSession: SparkSession,
                            datasetName: String,
                            sql: String,
                            incomingLogger: Option[Logger] = None): Boolean = {
    Try {
      import com.paypal.gimel.parser.utilities.QueryParserUtils._
      val transformedSQL = transformUdcSQLtoJdbcSQL(sql, getDatasets(sql))
      log(s"Validating push down query for sql: $transformedSQL", logger, incomingLogger)
      val jdbcConnectionUtility = JDBCConnectionUtility(sparkSession,
        DataSetUtils.getJdbcConnectionOptionsFromDataset(datasetName, sparkSession.conf.getAll))
      isPushDownEnabled(transformedSQL, jdbcConnectionUtility, incomingLogger)
    } match {
      case Success(value) => value
      case Failure(exception) =>
        logError(exception.getMessage, exception, logger, incomingLogger)
        false
    }
  }

  def isPushDownEnabled(transformedSQL: String,
                        jdbcConnectionUtility: JDBCConnectionUtility,
                        incomingLogger: Option[Logger]): Boolean = {
    val explainPlan =
      JDBCConnectionUtility.withResources(jdbcConnectionUtility.getJdbcConnectionAndSetQueryBand(incomingLogger)) {
        connection =>
          jdbcConnectionUtility.jdbcSystem match {
            case JdbcConstants.TERADATA =>
              executeExplainPlan(connection, transformedSQL)
            case JdbcConstants.MYSQL =>
              if (QueryParserUtils.isQueryOfGivenSeqType(transformedSQL, QueryConstants.IS_DDL_QUERY)) {
                logger.info("Found a MYSQL DDL query, hence skipping push down validation")
                GimelConstants.EXPLAIN_CONTEXT
              } else {
                executeExplainPlan(connection, transformedSQL)
              }
            case JdbcConstants.POSTGRESQL =>
              if (QueryParserUtils.isQueryOfGivenSeqType(transformedSQL, QueryConstants.IS_DDL_QUERY)) {
                logger.info("Found a POSTGRES DDL query, hence skipping push down validation")
                GimelConstants.EXPLAIN_CONTEXT
              } else {
                executeExplainPlan(connection, transformedSQL)
              }
            case JdbcConstants.ORCALE =>
              if (QueryParserUtils.isQueryOfGivenSeqType(transformedSQL, QueryConstants.IS_DDL_QUERY)) {
                logger.info("Found a QRACLE DDL query, hence skipping push down validation")
                GimelConstants.EXPLAIN_CONTEXT
              } else {
                executeOracleExplainPlan(connection, transformedSQL)
              }
            case _ =>
              log(s"Not a valid JDBC system, so cannot execute explain plan and hence returning empty string",
                logger, incomingLogger)
              GimelConstants.EMPTY_STRING
          }
      }
    val isPushDownEnabled = GenericUtils.isStrNotEmpty(explainPlan)
    log(s"Push down validation: $isPushDownEnabled", logger, incomingLogger)
    isPushDownEnabled
  }

  def parseNumeric(regex: Regex, incomingText: String, logger: Option[Logger] = None): BigInt = {
    getLastMatchingPattern(regex, incomingText) match {
      case regex(numericString) =>
        val rows = Try(
          BigInt(
            numericString
              .replaceAll(GimelConstants.COMMA, GimelConstants.EMPTY_STRING)
          )
        ).getOrElse(GimelConstants.ONE_BIGINT)
        rows
      case _ =>
        if (logger.isDefined) {
          logger.get.info("In default matcher")
        }
        GimelConstants.ONE_BIGINT
    }
  }

  def parseConfidence(regex: Regex,
                      incomingText: String,
                      logger: Option[Logger] = None,
                      defaultResult: GimelConstants.ConfidenceIdentifier = GimelConstants.NoConfidence
                     ): GimelConstants.ConfidenceIdentifier = {
    getLastMatchingPattern(regex, incomingText) match {
      case regex(string) =>
        string match {
          case GimelConstants.LOW_CONFIDENCE_IDENTIFIER => GimelConstants.LowConfidence
          case GimelConstants.HIGH_CONFIDENCE_IDENTIFIER => GimelConstants.HighConfidence
          case GimelConstants.NO_CONFIDENCE_IDENTIFIER => GimelConstants.NoConfidence
          case GimelConstants.INDEX_JOIN_CONFIDENCE_IDENTIFIER => GimelConstants.IndexJoinConfidence
          case _ => defaultResult
        }
      case _ =>
        if (logger.isDefined) {
          logger.get.info("In default matcher")
        }
        defaultResult
    }
  }

  private def getLastMatchingPattern(regex: Regex, incomingText: String): String = {
    val fs: Regex.MatchIterator = regex.findAllIn(incomingText)
    var next: String = GimelConstants.EMPTY_STRING
    while (fs.hasNext) {
      next = fs.next()
    }
    next
  }

  val DEFAULT_ROUNDING_MODE = RoundingMode.HALF_UP
  val ONE_MILLION: Long = 1000000L
  val JDBC_MAX_ROW_LIMIT: Long = 20000L
  val FAST_EXPORT_MAX_ROW_LIMIT: Long = 100000L
  val MAX_FETCH_SIZE: Int = 10000

  def computeConnectionDetails(
                                rows: Long,
                                overallSizeOfRecords: Long,
                                explainPlan: String,
                                maxReadOrWritePartitions: Int = JdbcConstants.MAX_TD_JDBC_READ_PARTITIONS,
                                operationMethod: String = JdbcConstants.READ_OPERATION,
                                logger: Option[Logger] = None,
                                confidence: ConfidenceIdentifier = GimelConstants.NoConfidence,
                                errorLimit: Option[Int] = None
                              ): ConnectionDetails = {
    // As of now confidence level, is not used for determining the partitions
    val perRowSize: Int =
      LongMath.divide(overallSizeOfRecords, rows, DEFAULT_ROUNDING_MODE).toInt
    if (logger.isDefined) {
      logger.get.info(s"No of rows $rows")
      logger.get.info(s"per row size $perRowSize")
      logger.get.info(s"Confidence level received: $confidence")
    }
    val fetchSize = Math.abs(
      Math.max(Math.min(
        Try(IntMath.divide(1024, perRowSize, DEFAULT_ROUNDING_MODE))
          .getOrElse(MAX_FETCH_SIZE),
        MAX_FETCH_SIZE
      ), 1)
    )
    if (rows < ONE_MILLION) {
      // Max to avoid getting default as 0
      val partitions = Math.max(
        Math.min(
          LongMath.divide(rows, JDBC_MAX_ROW_LIMIT, DEFAULT_ROUNDING_MODE),
          maxReadOrWritePartitions
        ).toInt, 1)
      ConnectionDetails(partitions, fetchSize, ConnectionDetails.JDBC_CONNECTION_URL_TYPE, errorLimit)
    } else {
      val partitions = Math.max(
        Math.min(
          LongMath
            .divide(rows, FAST_EXPORT_MAX_ROW_LIMIT, DEFAULT_ROUNDING_MODE),
          maxReadOrWritePartitions
        ).toInt, 1)
      operationMethod match {
        case JdbcConstants.READ_OPERATION =>
          ConnectionDetails(partitions, fetchSize, ConnectionDetails.FASTEXPORT_CONNECTION_URL_TYPE, errorLimit)
        case JdbcConstants.WRITE_OPERATION =>
          ConnectionDetails(partitions, fetchSize, ConnectionDetails.FASTLOAD_CONNECTION_URL_TYPE, errorLimit)
      }
    }
  }

  def validateMaxRowsToBeProcessed(sparkSession: SparkSession,
                                   rows: Long,
                                   logger: Option[Logger] = None,
                                   confidence: ConfidenceIdentifier): Unit = {
    if (GimelConstants.HighConfidence == confidence) {
      val userSuppliedPerProcessMaxJdbcRowsLimit = Try(
        sparkSession.conf.get(JdbcConfigs.jdbcPerProcessMaxRowsThreshold,
          sparkSession.conf.get(s"spark.${JdbcConfigs.jdbcPerProcessMaxRowsThreshold}",
            JdbcConstants.DEFAULT_JDBC_PER_PROCESS_MAX_ROWS_LIMIT_STRING)).toLong
      ).getOrElse(JdbcConstants.DEFAULT_JDBC_PER_PROCESS_MAX_ROWS_LIMIT)
      if (logger.isDefined) {
        logger.get.info(s"User specified per process max JDBC row limit:" +
          s" $userSuppliedPerProcessMaxJdbcRowsLimit")
        logger.get.info(s"Confidence level received: $confidence")
      }
      if (rows > userSuppliedPerProcessMaxJdbcRowsLimit) {
        val message = s"Terminating the process as the number of rows: $rows  " +
          s"exceeds permissible max rows limit: $userSuppliedPerProcessMaxJdbcRowsLimit"
        if (logger.isDefined) {
          logger.get.info(message)
        }
        throw new IllegalStateException(s"$message, " +
          s"Please reach out to gimel team[help-gimel] on the next steps on this process")
      }
    }
  }

  /**
    * Helper method for decoding teradata query specific connection information
    *
    * @param sparkSession
    * @param jdbcConnectionUtility
    * @param dataSetProps
    * @param sqlToBeExecuted
    * @param logger
    * @return
    */
  def getConnectionInfo(sparkSession: SparkSession, jdbcConnectionUtility: JDBCConnectionUtility,
                        dataSetProps: Map[String, Any], sqlToBeExecuted: String,
                        logger: Option[Logger] = None,
                        partitionColumns: Seq[String] = Seq.empty): (ConnectionDetails, JDBCConnectionUtility) = {
    if (logger.isDefined) {
      logger.get.info(s"Computing connection info for the incoming SQL: $sqlToBeExecuted")
    }
    // Per https://stackoverflow.com/questions/19154117/startswith-method-of-string-ignoring-case
    if (!QueryParserUtils.isSelectQuery(sqlToBeExecuted)) {
      val message = s"Cannot proceed with getting connection info for the given non select queries -> $sqlToBeExecuted"
      if (logger.isDefined) {
        logger.get.info(message)
      }
      throw new IllegalStateException(message)
    }
    val connectionDetails =
      JdbcAuxiliaryUtilities.getConnectionDetailsFromExplainPlan(sparkSession,
        getOrCreateConnection(jdbcConnectionUtility),
        sqlToBeExecuted,
        logger,
        partitionColumns)
    if (logger.isDefined) {
      logger.get.info(s"[ConnectionDetails]: $connectionDetails")
    }

    // Create new connection utility from the incoming resolved connection details
    val connectionUtilityPerIncomingSQL: JDBCConnectionUtility = connectionDetails.jdbcUrlType match {
      case ConnectionDetails.JDBC_CONNECTION_URL_TYPE =>
        JDBCConnectionUtility(sparkSession,
          dataSetProps - JdbcConfigs.teradataReadType - JdbcConfigs.teradataWriteType,
          Some(connectionDetails))
      case ConnectionDetails.FASTEXPORT_CONNECTION_URL_TYPE =>
        JDBCConnectionUtility(
          sparkSession,
          dataSetProps + (JdbcConfigs.teradataReadType -> connectionDetails.jdbcUrlType),
          Some(connectionDetails)
        )
      case ConnectionDetails.FASTLOAD_CONNECTION_URL_TYPE =>
        JDBCConnectionUtility(
          sparkSession,
          dataSetProps + (JdbcConfigs.teradataWriteType -> connectionDetails.jdbcUrlType),
          Some(connectionDetails)
        )
    }

    if (logger.isDefined) {
      logger.get.info(s"received user specified partitions -> ${dataSetProps.get("numPartitions")}, " +
        s"overriding it to ${connectionDetails.numOfPartitions}")
      logger.get.info(s"received user specified fetch size -> ${dataSetProps.get("fetchSize")}, " +
        s"overriding it to ${connectionDetails.fetchSize}")
    }
    (connectionDetails, connectionUtilityPerIncomingSQL)
  }

  /**
    * Utility used in both JDBC read and QueryPushDown to set the partition sepecific informations required for teradata
    *
    * @param sparkSession
    * @param dataSetProps
    * @param userSpecifiedFetchSize
    * @param mutableJdbcOptions
    * @param connection
    * @param logger
    * @return
    */
  def getAndSetPartitionParameters(sparkSession: SparkSession, dataSetProps: Map[String, Any],
                                   userSpecifiedFetchSize: Int, mutableJdbcOptions: mutable.Map[String, String],
                                   connection: Connection, logger: Option[Logger] = None): Seq[String] = {
    import PartitionUtils._
    if (dataSetProps.contains(JdbcConfigs.jdbcPartitionColumns)) {
      mutableJdbcOptions += (JdbcConfigs.jdbcPartitionColumns -> dataSetProps(
        JdbcConfigs.jdbcPartitionColumns
      ).toString)
    }
    //    sparkSession.spark.setLocalProperty(JdbcConfigs.jdbcPushDownEnabled, "true")
    getPartitionColumns(mutableJdbcOptions.toMap, connection)
  }

  /**
    * Utility for performing query push down
    *
    * @param sql
    * @param connection
    * @return
    */
  def executeQueryAndReturnResultString(sql: String, connection: Connection): String = {
    sql match {
      case _ if QueryParserUtils.isQueryOfGivenSeqType(sql) =>
        JdbcAuxiliaryUtilities.executeQuery(connection, sql)
      case _ if QueryParserUtils.isQueryOfGivenSeqType(sql, QueryConstants.IS_DDL_DML_QUERY) =>
        JdbcAuxiliaryUtilities.executeUpdate(connection, sql)
      case _ => JdbcAuxiliaryUtilities.executeQuery(connection, sql, isUpdatable = true)
    }
  }

  /**
    *
    * @param sparkSession
    * @param sqlToBeExecutedInJdbcRDD
    * @param fetchSize
    * @param numOfPartitions
    * @param connectionUtilityPerIncomingSQL
    * @param partitionColumns
    * @param tableSchema
    * @return
    */
  def createJdbcDataFrame(sparkSession: SparkSession, sqlToBeExecutedInJdbcRDD: String,
                          fetchSize: Int, numOfPartitions: Int,
                          connectionUtilityPerIncomingSQL: JDBCConnectionUtility,
                          partitionColumns: Seq[String], tableSchema: StructType): DataFrame = {
    import PartitionUtils._
    val jdbcRDD: ExtendedJdbcRDD[Array[Object]] = new ExtendedJdbcRDD(sparkSession.sparkContext,
      new DbConnection(connectionUtilityPerIncomingSQL), sqlToBeExecutedInJdbcRDD, fetchSize,
      PartitionInfoWrapper(JdbcConstants.TERADATA, partitionColumns, JdbcConstants.DEFAULT_LOWER_BOUND,
        JdbcConstants.DEFAULT_UPPER_BOUND, numOfPartitions = numOfPartitions))

    val rowRDD: RDD[Row] = jdbcRDD.map(Row.fromSeq(_))
    sparkSession.createDataFrame(rowRDD, tableSchema)
  }
}
