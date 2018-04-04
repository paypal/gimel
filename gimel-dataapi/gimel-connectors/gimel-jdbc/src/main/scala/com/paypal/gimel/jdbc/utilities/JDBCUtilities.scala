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

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}


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
  val fastloadBatchSize = 0
  val NUM_READ_PARTITIONS = 4

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
    val jdbcOptions: Map[String, String] = getJDBCOptions(dataSetProps)
    val teradataType: String = dataSetProps.getOrElse(JdbcConfigs.teradataReadType, "").toString
    teradataType match {
      case "FASTEXPORT" =>
        readwithFastExport(dataSetProps)
      case _ =>
        readWithJdbcLoad(jdbcOptions)
    }
  }

  /**
    * This function reads data with specified parameters from JDBC datasource table and returns it as a dataFrame
    *
    * @param jdbcOptions This parameter specifies all the configuration parameter to read the data from  given table
    * @return DataFrame
    */
  private def readWithJdbcLoad(jdbcOptions: Map[String, String]): DataFrame = {
    val jdbcREADOptions = jdbcOptions + ("charset" -> "ASCII")
    // Read data from JDBC datasource with specified parameters
    sparkSession
      .read
      .format("jdbc")
      .options(jdbcREADOptions)
      .load()
  }

  /**
    * Method to get the JDBC url given the parameters map
    *
    * @param dataSetProps dataset properties to specify additional parameters
    * @return jdbc_url
    *         JDBC data source url as a string
    */
  private def getjdbcReadURL(dataSetProps: Map[String, Any]): String = {
    val jdbcOptions: Map[String, String] = getJDBCOptions(dataSetProps)
    val jdbcUrl = jdbcOptions("url")
    val charset = dataSetProps.getOrElse("charset", "ASCII").toString
    val teradataType: String = dataSetProps(JdbcConfigs.teradataReadType).toString
    val teradataSessions: String = dataSetProps.getOrElse("SESSIONS", "5").toString
    // Get specific URL properties
    constructJDBCUrl(jdbcUrl, charset, teradataType, teradataSessions)
  }

  /**
    * A method that will read data using FASTEXPORT from Teradata data source
    *
    * @param dataSetProps dataset peroperties to specify addiotional parameters
    */
  private def readwithFastExport(dataSetProps: Map[String, Any]) = {
    val jdbcURL = getjdbcReadURL(dataSetProps)
    val jdbcOptions: Map[String, String] = getJDBCOptions(dataSetProps)
    val dbConnection = new DbConnection(jdbcURL, jdbcOptions.get("user").get, jdbcOptions.get("password").get)
    val dbtable = jdbcOptions("dbtable")
    val partitions: Int = dataSetProps.getOrElse("partitions", NUM_READ_PARTITIONS).toString.toInt
    val partitionColumn = dataSetProps.getOrElse("partitionColumn", "10")
    val lowerBound = dataSetProps.getOrElse("columnLowerBound", 0).toString.toInt
    val upperBound = dataSetProps.getOrElse("columnUpperBound", 20).toString.toInt
    val selectStmt = s"SELECT * FROM $dbtable WHERE ?<=$partitionColumn AND $partitionColumn<=?"
    val jdbcRDD = new JdbcRDD(sparkSession.sparkContext, dbConnection, selectStmt, lowerBound, upperBound, partitions)
    val hiveTableSchema: StructType = getHiveSchema(dataSetProps("dataSet").toString)
    val rowRDD = jdbcRDD.map(v => Row(v: _*))
    sparkSession.createDataFrame(rowRDD, hiveTableSchema)
  }

  /**
    * This method returns the hive schema for a given hive table as StructType
    *
    * @param hiveTable The hive table name
    * @return StructType
    *         The hive schema for the given hive table
    */
  private def getHiveSchema(hiveTable: String): StructType = {
    val table: DataFrame = sparkSession.sql(s"SELECT * FROM $hiveTable")
    table.schema
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
    val jdbcOptions: Map[String, String] = getJDBCOptions(dataSetProps)
    var jdbc_url = jdbcOptions("url")
    val charset = dataSetProps.getOrElse("charset", "ASCII").toString
    val username = jdbcOptions("user")
    val password = jdbcOptions("password")
    val teradataSessions: String = dataSetProps.getOrElse("SESSIONS", "5").toString
    val batchSize: Int = dataSetProps.getOrElse("BATCHSIZE", "0").toString.toInt
    val dbtable = jdbcOptions("dbtable")
    val teradataType: String = dataSetProps.getOrElse(JdbcConfigs.teradataWriteType, "").toString
    val insertStrategy: String = dataSetProps.getOrElse(JdbcConfigs.jdbcInsertStrategy, "insert").toString
    val insertPartitionsCount = dataSetProps.getOrElse(JdbcConfigs.jdbcInputDataPartitionCount, "2").toString
    // Get specific URL properties
    jdbc_url = constructJDBCUrl(jdbc_url, charset, teradataType, teradataSessions)
    val partialArgHolder = JDBCArgsHolder(dataSetProps, dataFrame.columns, jdbc_url, username, password, dbtable, _: Int, dataFrame.schema.length)

    if (insertStrategy == "FullLoad") {
      val dbconn: Connection = getConnection(jdbc_url, username, password)
      // truncate table
      truncateTable(dbtable, dbconn)
    }

    val dfCoalesceFunction = (df: DataFrame) => df.coalesce(1)

    val dfIdentityFunction = (df: DataFrame) => df.coalesce(insertPartitionsCount.toInt)

    val (insetBatchSize, insertMethod, coalesceMethod) = (insertStrategy, teradataType) match {
      case ("update", _) =>
        (0, updateTable _, dfIdentityFunction)
      case ("FullLoad", "FASTLOAD") =>
        (fastloadBatchSize, insertBatch _, dfCoalesceFunction)
      case ("FullLoad", _) =>
        (batchSize, insertBatch _, dfIdentityFunction)
      case ("append", _) =>
        (0, updateAndInsertTable _, dfIdentityFunction)
      case ("insert", "FASTLOAD") =>
        (fastloadBatchSize, insertBatch _, dfCoalesceFunction)
      case ("insert", _) =>
        (batchSize, insertBatch _, dfIdentityFunction)
    }

    val sourceDataFrame = coalesceMethod(dataFrame)

    val jdbcHolder = partialArgHolder(insetBatchSize)
    insertMethod(sourceDataFrame, jdbcHolder)

    dataFrame
  }

  /**
    * This method returns JDBC connection given the URL & credentials
    *
    * @param url      JDBC data source URL to connect
    * @param username username for JDBC data source
    * @param password password for JDBC data source
    * @return Connection
    */
  def getConnection(url: String, username: String, password: String): Connection = {
    DriverManager.getConnection(url, username, password)
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
    * This method truncates the given table give the connection to JDBC data source
    *
    * @param dbTable table name
    * @param dbconn  JDBC data source connection
    * @return Boolean
    */
  def truncateTable(dbTable: String, dbconn: Connection): Boolean = {
    val truncateTableStatement = s"DELETE ${dbTable} ALL"
    val st: PreparedStatement = dbconn.prepareStatement(truncateTableStatement)
    st.execute()
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
    * This method updates the given table
    *
    * @param dataFrame  dataFrame to be loaded into table
    * @param jdbcHolder jdbc arguments required for writing into table
    */
  private def updateTable(dataFrame: DataFrame, jdbcHolder: JDBCArgsHolder) {
    dataFrame.foreachPartition { batch =>
      if (batch.nonEmpty) {
        val dbc: Connection = DriverManager.getConnection(jdbcHolder.jdbcURL, jdbcHolder.user, jdbcHolder.password)
        try {
          val primaryKeys: List[String] = getPrimaryKeys(jdbcHolder.dbTable, dbc)
          val st: PreparedStatement = dbc.prepareStatement(getUpdateStatement(jdbcHolder.dbTable, jdbcHolder.dataFrameColumns, primaryKeys))
          val maxBatchSize = math.max(1, jdbcHolder.batchSize)
          val primaryKeyIndices = primaryKeys.map(getColumnIndex(jdbcHolder.dataFrameColumns, _))
          batch.sliding(maxBatchSize, maxBatchSize).foreach { rows =>
            rows.foreach { row =>
              cookStatementWithRow(st, row, 0 until row.schema.length, 1 to row.schema.length)
              // set the condition variables
              val whereParamIndices = (1 to primaryKeyIndices.length).map(_ + row.schema.length)
              cookStatementWithRow(st, row, primaryKeyIndices, whereParamIndices)
              st.addBatch()
            }
            st.executeBatch()
          }
        } finally {
          dbc.close()
        }
      }
    }
  }


  /**
    * This method inserts into given table in given mode
    *
    * @param dataFrame  dataFrame to be loaded into table
    * @param jdbcHolder jdbc arguments required for writing into table
    */
  private def insertBatch(dataFrame: DataFrame, jdbcHolder: JDBCArgsHolder) {
    dataFrame.foreachPartition { batch =>
      if (batch.nonEmpty) {
        val dbc: Connection = DriverManager.getConnection(jdbcHolder.jdbcURL, jdbcHolder.user, jdbcHolder.password)
        try {
          val st = dbc.prepareStatement(getInsertStatement(jdbcHolder.dbTable, jdbcHolder.cols))
          val maxBatchSize = math.max(1, jdbcHolder.batchSize)

          batch.sliding(maxBatchSize, maxBatchSize).foreach { rows =>
            rows.foreach { row =>
              cookStatementWithRow(st, row, 0 until row.schema.length, 1 to row.schema.length)
              st.addBatch()
            }
            st.executeBatch()
          }
        } finally {
          dbc.close()
        }
      }
    }
  }

  /**
    * This method appends into table - updates row if the condition matches else insert the row into table
    *
    * @param dataFrame source dataframe
    * @param jdbcArgs  jdbcholder object
    */
  private def updateAndInsertTable(dataFrame: DataFrame, jdbcArgs: JDBCArgsHolder) {
    dataFrame.foreachPartition { batch =>
      if (batch.nonEmpty) {
        val dbc = DriverManager.getConnection(jdbcArgs.jdbcURL, jdbcArgs.user, jdbcArgs.password)
        try {
          val numCols: Int = jdbcArgs.cols
          val primaryKeys: List[String] = getPrimaryKeys(jdbcArgs.dbTable, dbc)
          val stStr = getUpdateStatement(jdbcArgs.dbTable, jdbcArgs.dataFrameColumns, primaryKeys)
          val st: PreparedStatement = dbc.prepareStatement(stStr)
          val primaryKeyIndices = primaryKeys.map { pk =>
            getColumnIndex(jdbcArgs.dataFrameColumns, pk)
          }
          batch.foreach { row =>
            cookStatementWithRow(st, row, 0 until row.schema.length, 1 to row.schema.length)
            // set the condition variables
            val paramIndices = (1 to primaryKeyIndices.length).map(_ + row.schema.length)
            cookStatementWithRow(st, row, primaryKeyIndices, paramIndices)
            // update table
            val updateResult: Int = st.executeUpdate()
            // if no update, then insert into table
            if (updateResult == 0) {
              val st = dbc.prepareStatement(getInsertStatement(jdbcArgs.dbTable, numCols))
              cookStatementWithRow(st, row, 0 until row.schema.length, 1 to row.schema.length)
              // insert into table
              st.executeUpdate()
            }
          }
        } finally {
          dbc.close()
        }
      }
    }
  }

  /**
    * This function returns the JDBC properties map from HIVE table properties
    *
    * @param dataSetProps This will set all thew options required for READ/WRITE from/to JDBC data source
    * @return jdbcOptions
    */
  private def getJDBCOptions(dataSetProps: Map[String, Any]): Map[String, String] = {
    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val hiveTableParams = datasetProps.props
    // url
    val url = hiveTableParams(JdbcConfigs.jdbcUrl).toString
    // driver
    val driver = hiveTableParams(JdbcConfigs.jdbcDriverClassKey).toString
    // table
    val jdbcTableName = hiveTableParams(JdbcConfigs.jdbcInputTableNameKey).toString
    // Get username and password
    val (username, password) = authUtilities.getJDBCCredentials(url, dataSetProps)
    // Set all the options as scala map to pass to read/write methods
    val jdbcOptions: Map[String, String] = Map("url" -> url, "driver" -> driver, "dbtable" -> jdbcTableName, "user" -> username, "password" -> password)
    jdbcOptions
  }

  /**
    * This method generates the INSERT statement dynamically
    *
    * @param dbtable tablename of the JDBC data source
    * @param cols    Number of columns in the table
    * @return insertStatement
    *         Insert statement as a String
    */
  private def getInsertStatement(dbtable: String, cols: Int): String = {
    val statementStart = s"INSERT INTO $dbtable  VALUES( "
    val statementEnd = ")"
    val separator = ", "

    Seq.fill(cols)("?").mkString(statementStart, separator, statementEnd)
  }

  /**
    * This method generates the UPADTE statement dynamically
    *
    * @param dbtable          tablename of the JDBC data source
    * @param dataFrameColumns Array of dataframe columns
    * @return updateStatement
    *         Update statement as a String
    */
  private def getUpdateStatement(dbtable: String, dataFrameColumns: Array[String], primaryKeys: List[String]) = {
    require(dataFrameColumns.nonEmpty, s"Column names cannot be an empty array for table $dbtable.")
    require(primaryKeys.nonEmpty, s"The set of primary keys cannot be empty for table $dbtable.")
    val setColumns = dataFrameColumns.map(columnName => s"${columnName} = ?")
    val separator = ", "
    val statementStart = s"UPDATE $dbtable SET "
    val whereConditions = primaryKeys.map { key => s"${key} = ?" }
    val whereClause = whereConditions.mkString(" WHERE ", " AND ", "")
    setColumns.mkString(statementStart, separator, whereClause)
  }


  def getStringColumn(resultSet: ResultSet, columnIndex: Int): Option[String] = {
    if (resultSet.next()) {
      Some(resultSet.getString(columnIndex))
    } else {
      None
    }
  }

  /**
    * This method returns the primary keys of the table
    *
    * @param dbTable table name
    * @param dbConn  database connection
    * @return List of primary keys
    */
  def getPrimaryKeys(dbTable: String, dbConn: Connection): List[String] = {
    val Array(databaseName, tableName) = dbTable.split("""\.""")
    val primaryKeyStatement: String =
      s"""SELECT ColumnName FROM dbc.IndicesV
          |WHERE DatabaseName = '$databaseName'
          |AND TableName = '$tableName'
          |AND IndexType = 'P' """.stripMargin
    val st: PreparedStatement = dbConn.prepareStatement(primaryKeyStatement)
    val resultSet: ResultSet = st.executeQuery()
    Iterator
      .continually(getStringColumn(resultSet, columnIndex = 1))
      .takeWhile(_.nonEmpty)
      .flatten
      .toList
  }

  /** This method generates the abosolute URL for JDBC data source
    *
    * @param jdbc_url         JDBC URL
    * @param charset          character set to be set
    * @param teradataType     LOAD/EXPORT type e.g FASTLOAD/FASTEXPORT
    * @param teradataSessions Number of sessions required
    * @return JDBC URL along with all parameters
    */
  private def constructJDBCUrl(jdbc_url: String, charset: String, teradataType: String, teradataSessions: String): String = {
    var newURL = jdbc_url
    // charset
    newURL = newURL + "/" + "charset=" + charset
    // type
    if (teradataType.equals("FASTLOAD") || teradataType.equals("FASTEXPORT")) {
      newURL = newURL + "," + "TYPE=" + teradataType
      //  sessions
      newURL = newURL + "," + "SESSIONS=" + teradataSessions
    }
    newURL
  }

}
