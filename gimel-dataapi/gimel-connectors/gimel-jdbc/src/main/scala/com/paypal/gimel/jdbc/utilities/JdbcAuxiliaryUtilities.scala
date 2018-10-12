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

import scala.collection.immutable.Map

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

import com.paypal.gimel.common.catalog.{DataSetProperties, Field}
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}

object JdbcAuxiliaryUtilities {

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
    *
    * @param databaseName
    * @param tableName
    * @param dbConn
    * @param NUMERIC_ONLY_FLAG
    * @return
    */
  def getPrimaryKeysForTeradata(databaseName: String, tableName: String, dbConn: Connection, NUMERIC_ONLY_FLAG: Boolean): List[String] = {
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

    val updatedPrimaryKeyStatement = NUMERIC_ONLY_FLAG match {

      case true =>
          s"""
             | ${primaryKeyStatement}
             | AND ColumnType in ('I','I1','I2','I8', 'F', 'D', 'DA', 'BF', 'BV')
             |
           """.stripMargin

      case _ =>
        primaryKeyStatement
    }

    val st: PreparedStatement = dbConn.prepareStatement(updatedPrimaryKeyStatement)
    val resultSet: ResultSet = st.executeQuery()

    Iterator
      .continually(getStringColumn(resultSet, columnIndex = 1))
      .takeWhile(_.nonEmpty)
      .flatten
      .toList
  }

  /**
    *
    * @param databaseName
    * @param tableName
    * @param dbConn
    * @param NUMERIC_ONLY_FLAG
    * @return
    */
  def getPrimaryKeysForMysql(databaseName: String, tableName: String, dbConn: Connection, NUMERIC_ONLY_FLAG: Boolean): List[String] = {
    val primaryKeyStatement: String =
      s"""
         |SELECT COLUMN_NAME as ColumnName
         |FROM information_schema.COLUMNS
         |WHERE (TABLE_SCHEMA = '${databaseName}')
         |AND (TABLE_NAME = '${tableName}')
         |AND (COLUMN_KEY = 'PRI')
         |
       """.stripMargin

    val updatedPrimaryKeyStatement = NUMERIC_ONLY_FLAG match {

      case true =>
        s"""
           | ${primaryKeyStatement}
           | AND DATA_TYPE in ('INT', 'SMALLINT', 'TINYINT', 'MEDIUMINT', 'MEDIUMINT', 'FLOAT', 'DOUBLE')
           |
           """.stripMargin

      case _ =>
        primaryKeyStatement
    }

    val st: PreparedStatement = dbConn.prepareStatement(updatedPrimaryKeyStatement)
    val resultSet: ResultSet = st.executeQuery()

    Iterator
      .continually(getStringColumn(resultSet, columnIndex = 1))
      .takeWhile(_.nonEmpty)
      .flatten
      .toList
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
  def getPrimaryKeys(url: String, dbTable: String, dbConn: Connection, NUMERIC_ONLY_FLAG: Boolean = false): List[String] = {
    val Array(databaseName, tableName) = dbTable.split("""\.""")

    val jdbcSystem = getJDBCSystem(url)
    jdbcSystem match {
      case JdbcConstants.TERADATA =>
        getPrimaryKeysForTeradata(databaseName, tableName, dbConn, NUMERIC_ONLY_FLAG)

      case JdbcConstants.MYSQL =>
        getPrimaryKeysForMysql(databaseName, tableName, dbConn, NUMERIC_ONLY_FLAG)

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
    * @param dbtable          tablename of the JDBC data source
    * @param setColumns Array of dataframe columns
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
    val getMinMaxStatement = s"""SELECT MIN(${column}) as lowerBound, MAX(${column}) as upperBound FROM ${dbTable}"""
    val columnStatement: PreparedStatement = conn.prepareStatement(getMinMaxStatement)
    try {
      val resultSet: ResultSet = columnStatement.executeQuery()
      if (resultSet.next()) {
        val lowerBound = resultSet.getObject("lowerbound")
        val upperBound = resultSet.getObject("upperbound")
        if (lowerBound != null && upperBound!= null ) {
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
        throw new Exception(s"Error getting min & max value from ${dbTable}: ${e.getMessage}", e)
    }
  }

  /**
    *
    * @param dbtable
    * @param con
    */
  def dropTable(dbtable: String, con: Connection): Unit = {
    val dropStatement =
      s"""
         | DROP TABLE ${dbtable}
       """.stripMargin
    try {
      val dropPmt = con.prepareStatement(dropStatement)
      dropPmt.execute()
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  /**
    *
    * @param dbTable       table
    * @param con           connection
    * @param numPartitions num of partitions
    * @return
    */
  def insertPartitionsIntoTargetTable(dbTable: String, con: Connection, numPartitions: Int): Boolean = {

    val insertStatement =
      s"""
         | INSERT INTO ${dbTable}
         | ${getUnionAllStatetementFromPartitions(dbTable, numPartitions)}
       """.stripMargin

    val insertUnion: PreparedStatement = con.prepareStatement(insertStatement)
    insertUnion.execute()
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
      s"${selStmt} ${dbTable}_${JdbcConstants.GIMEL_TEMP_PARTITION}_${partitionId}"
    ).mkString(" UNION ALL ")
  }

  /**
    *
    * @param dbtable
    * @param con
    * @param numPartitions
    */
  def dropAllPartitionTables(dbtable: String, con: Connection, numPartitions: Int): Unit = {
    (0 until numPartitions).map(partitionId =>
      JdbcAuxiliaryUtilities.dropTable(s"${dbtable}_${JdbcConstants.GIMEL_TEMP_PARTITION}_${partitionId}", con)
    )
  }


  /**
    *
    * @param query string
    * @param con jdbc connection
    * @return
    */
  def executeQuerySatement(query: String, con: Connection): Boolean = {
    val queryStatement: PreparedStatement = con.prepareStatement(query)
    queryStatement.execute()
  }


  /**
    *
    * @param dbTable jdbc tablename
    * @param con     jdbc connection
    * @return ddl fo the table
    */
  def getDDL(dbTable: String, con: Connection): String = {

    var buf = new StringBuilder

    val showViewQuery: String = s"SHOW TABLE ${dbTable}"

    val showViewStatement: PreparedStatement = con.prepareStatement(showViewQuery)
    val resultSet = showViewStatement.executeQuery()
    while (resultSet.next()) {
      buf ++= resultSet.getString(1).replaceAll("[\u0000-\u001f]", " ").replaceAll(" +", " ")
    }
    buf.toString
  }

  /**
    * This returns the dataset properties based on the attribute set in the properties map
    *
    * @param dataSetProps
    * @return
    */
  def getDataSetProperties(dataSetProps: Map[String, Any]): Map[String, Any] = {
    val dsetProperties: Map[String, Any] = dataSetProps.contains(GimelConstants.DATASET_PROPS)  match {
      case true =>
        // adding the properties map received from metadata to the outer map as well
        val datasetPropsOption: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
        datasetPropsOption.props ++ dataSetProps
      case _ =>
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
    // table
    val jdbcTableName = dsetProperties(JdbcConfigs.jdbcInputTableNameKey).toString

    val jdbcOptions: Map[String, String] = Map("url" -> url, "driver" -> driver, "dbtable" -> jdbcTableName)
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
    // table
    val jdbcTableName = dsetProperties(JdbcConfigs.jdbcInputTableNameKey).toString

    val jdbcOptions: Map[String, String] = Map("url" -> url, "driver" -> driver, "dbtable" -> jdbcTableName)
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

    // url
    val url = dsetProperties(JdbcConfigs.jdbcUrl).toString

    val jdbcSystem = getJDBCSystem(url)
    val jdbcOptions: Map[String, String] = jdbcSystem match {
      case JdbcConstants.TERADATA =>
        getTeradataOptions(dsetProperties)
      case JdbcConstants.MYSQL =>
        getMysqlOptions(dsetProperties)
      case _ =>
        Map[String, String]()
    }
    jdbcOptions
  }

  /**
    *
    * @param dataSetProps
    * @return string url
    */
  def getJdbcUrl(dataSetProps: Map[String, Any]): String = {

    val dsetProperties: Map[String, Any] = getDataSetProperties(dataSetProps)

    // get basic url
    val url = dsetProperties(JdbcConfigs.jdbcUrl).toString

    val jdbcSystem = getJDBCSystem(url)
    val jdbcUrl = jdbcSystem match {
      case JdbcConstants.TERADATA =>
        {
          val charset = dataSetProps.getOrElse("charset", JdbcConstants.deafaultCharset).toString
          val teradataReadType: String = dataSetProps.getOrElse(JdbcConfigs.teradataReadType, "").toString
          val teradataWriteType: String = dataSetProps.getOrElse(JdbcConfigs.teradataWriteType, "").toString

          // get sessions
          val teradataSessions: String = dataSetProps.getOrElse("SESSIONS", JdbcConstants.defaultSessions).toString

          var newURL = url + "/" + "charset=" + charset
          // Teradata READ type
          newURL = if (teradataReadType.toUpperCase.equals("FASTEXPORT")) {
            newURL + "," + "TYPE=FASTEXPORT" + "," + s"SESSIONS=${teradataSessions}"
          }
          else if (teradataWriteType.toUpperCase.equals("FASTLOAD")) {
            newURL + "," + "TYPE=FASTLOAD" + "," + s"SESSIONS=${teradataSessions}"
          }
          else {
            newURL
          }
          newURL
        }
      case JdbcConstants.MYSQL =>
        url
      case _ =>
        throw new Exception(s"This JDBC System is not supported. Please check gimel docs.")
    }
    jdbcUrl
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
      else {
        JdbcConstants.TERADATA
      }
    }
}
