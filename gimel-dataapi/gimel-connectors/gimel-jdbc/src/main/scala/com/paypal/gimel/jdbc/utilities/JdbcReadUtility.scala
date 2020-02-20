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

import java.sql.{Connection, JDBCType, ResultSet, ResultSetMetaData, SQLException}

import scala.math.min

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DecimalType.{MAX_PRECISION, MAX_SCALE}

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.{GenericUtils, SQLDataTypesUtils}
import com.paypal.gimel.jdbc.conf.JdbcConstants
import com.paypal.gimel.jdbc.utilities.JdbcAuxiliaryUtilities.getJDBCSystem
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.parser.utilities.QueryParserUtils

object JdbcReadUtility {

  private val GENERIC_FAILING_CONDITION_CLAUSE: String = "1=0"
  private val logger: Logger = Logger(this.getClass.getName)

  /**
    *
    * @param url
    * @param selectQuery
    * @return
    */
  def resolveTable(url: String, selectQuery: String, conn: Connection): StructType = {
    require(QueryParserUtils.isSelectQuery(selectQuery), s"Resolve table, " +
      s"expects select queries given $selectQuery is not a select")
    GenericUtils.time("ResolveTable: ", Some(logger)) {
      val dialect = org.apache.spark.sql.jdbc.JdbcDialects.get(url)
      import JDBCConnectionUtility.withResources
      val mergedSqlWithFailureConditionClause = SQLDataTypesUtils.mergeConditionClause(
        selectQuery, GENERIC_FAILING_CONDITION_CLAUSE
      )
      logger.info(s"In resolve table, proceeding to execute: $mergedSqlWithFailureConditionClause")
      withResources(conn.prepareStatement(mergedSqlWithFailureConditionClause)) {
        statement =>
          withResources(statement.executeQuery()) {
            resultSet => getSchema(url, resultSet, dialect, alwaysNullable = true)
          }
      }
    }
  }


  /**
    * @param url
    * @param resultSet
    * @param dialect
    * @param alwaysNullable
    * @return
    */
  def getSchema(url: String,
                resultSet: ResultSet,
                dialect: JdbcDialect,
                alwaysNullable: Boolean = false): StructType = {
    val rsmd = resultSet.getMetaData
    val ncols = rsmd.getColumnCount
    val fields = new Array[StructField](ncols)
    var i = 0
    while (i < ncols) {
      val columnName = rsmd.getColumnLabel(i + 1)
      val dataType: Int = rsmd.getColumnType(i + 1)
      val typeName: String = rsmd.getColumnTypeName(i + 1)
      val fieldSize = rsmd.getPrecision(i + 1)
      val fieldScale = rsmd.getScale(i + 1)
      val isSigned = {
        try {
          rsmd.isSigned(i + 1)
        } catch {
          // Workaround for HIVE-14684:
          case e: SQLException if
          e.getMessage == "Method not supported" &&
            rsmd.getClass.getName == "org.apache.hive.jdbc.HiveResultSetMetaData" => true
        }
      }
      val nullable = if (alwaysNullable) {
        true
      } else {
        rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
      }
      val metadata = new MetadataBuilder().putLong("scale", fieldScale)
      val columnType: DataType =
        dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
          getCatalystType(url, dataType, fieldSize, fieldScale, isSigned, typeName))

      // val columnType: DataType = getCatalystType(url, dataType, fieldSize, fieldScale, isSigned)
      fields(i) = StructField(columnName, columnType, nullable)
      i = i + 1
    }
    new StructType(fields)
  }

  /**
    * Maps a JDBC type to a Catalyst type.  This function is called only when
    * the JdbcDialect class corresponding to your database driver returns null.
    *
    * @param sqlType - A field of java.sql.Types
    * @return The Catalyst type corresponding to sqlType.
    */
  def getCatalystType(url: String,
                      sqlType: Int,
                      precision: Int,
                      scale: Int,
                      signed: Boolean,
                      typeName: String): DataType = {
    val answer = sqlType match {
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT => if (signed) {
        LongType
      } else {
        DecimalType(20, 0)
      }
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
      case java.sql.Types.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => {
        // Checking this specific case when trying for TERADATA FLOAT data types, returns DoubleType
        val jdbcSystem = getJDBCSystem(url)
        jdbcSystem match {
          case JdbcConstants.TERADATA =>
            DoubleType
          case _ =>
            FloatType
        }
      }
      case java.sql.Types.INTEGER => if (signed) {
        IntegerType
      } else {
        LongType
      }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
      case java.sql.Types.NUMERIC => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR => StringType
      // Fix for missing datatype from source, also update ExtendedJdbcRDD.resultSetToObjectArray
      case java.sql.Types.OTHER =>
        typeName match {
          case GimelConstants.TERADATA_JSON_COLUMN_TYPE => StringType
          case _ => null
        }
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.REF_CURSOR => null
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE => null
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => null
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ =>
        throw new SQLException("Unrecognized SQL type " + sqlType)
    }

    if (answer == null) {
      throw new SQLException("Unsupported type " + JDBCType.valueOf(sqlType).getName)
    }
    answer
  }


}
