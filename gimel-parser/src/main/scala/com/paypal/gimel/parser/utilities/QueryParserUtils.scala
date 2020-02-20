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

package com.paypal.gimel.parser.utilities

import java.sql.SQLException

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}

import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

object QueryParserUtils {

  private val logger: Logger =
    LoggerFactory.getLogger(QueryParserUtils.getClass)

  /**
    * Utility for traversing and getting all the UDC datasets specified in the given query
    *
    * @param sql -> Incoming SQL
    * @return
    */
  def getDatasets(sql: String, identifier: String = "udc"): Seq[String] = {
    logger.info(
      s" @Begin --> ${new Exception().getStackTrace.apply(1).getMethodName}"
    )
    tokenize(sql.toLowerCase).filter(_.startsWith(identifier))
  }

  /**
    * Returns the individual works from the SQL as tokens
    *
    * @param sql SqlString
    * @return String tokens
    */
  def tokenize(sql: String): Array[String] = {
    removeSQLComments(sql)
      .replaceAllLiterally("\n", " ")
      .replaceAllLiterally("\t", " ")
      .replaceAllLiterally(",", " , ")
      .replaceAllLiterally(";", " ; ")
      .replaceAllLiterally("=", " = ")
      .replaceAllLiterally("(", " ( ")
      .replaceAllLiterally(")", " ) ")
      .split(" ")
      .filter(x => !x.isEmpty)
  }

  /**
    *  Utility for transforming incoming UDC SQL to JDBC SQL
    *
    * @param sql -> SQL received from user
    * @param dataSets -> Seq of data set specified on the sql received from user
    * @return
    */
  def transformUdcSQLtoJdbcSQL(sql: String, dataSets: Seq[String]): String = {
    var transformedSQL = sql
    dataSets.foreach(
      datasetName =>
        transformedSQL = StringUtils.replaceIgnoreCase(
          transformedSQL,
          datasetName,
          extractTableName(datasetName)
      )
    )
    logger.info(s"Transformed SQL: $transformedSQL to be executed")
    transformedSQL
  }

  /**
    * Utility to extract the table name
    *
    * @param datasetName like udc.teradata.cluster_name.database_name.table_name
    * @return
    */
  def extractTableName(datasetName: String,
                       ordinal: Int = 2,
                       search: String = "."): String = {
    require(datasetName != null, "Expected dataset name")
    val index = StringUtils.lastOrdinalIndexOf(datasetName, ".", ordinal)
    if (index == -1) {
      throw new IllegalStateException(
        s"Table Name cannot be extracted for the incoming dataset: $datasetName, " +
          s"It should be of the format like udc.teradata.cluster_name.database_name.table_name"
      )
    }
    datasetName.substring(index + 1).trim
  }

  /**
    * Right trim the incoming string
    *
    * @param s -> incoming string
    * @return
    */
  def rtrim(s: String): String = s.replaceAll("\\s+$", "")

  /**
    * Utility for getting the system name from dataset
    *
    * @param datasetName like udc.teradata.cluster_name.database_name.table_name
    * @return
    */
  def extractSystemFromDatasetName(datasetName: String,
                                   ordinal: Int = 3,
                                   search: String = "."): String = {
    require(datasetName != null, "Expected dataset name")
    val index = StringUtils.ordinalIndexOf(datasetName, ".", ordinal)
    if (index == -1) {
      throw new IllegalStateException(
        s"System name cannot be extracted for the incoming dataset: $datasetName, " +
          s"It should be of the format like udc.teradata.cluster_name.database_name.table_name"
      )
    }
    datasetName.substring(datasetName.indexOf(search) + 1, index).trim
  }

  /**
    * Validates the SQL, if it starts with SELECT
    *
    * @param sql -> Incoming query
    * @return
    */
  def isSelectQuery(sql: String): Boolean = {
    QueryParserUtils
      .ltrim(sql)
      .regionMatches(
        true,
        0,
        QueryConstants.SQL_SELECT_STRING,
        0,
        QueryConstants.SQL_SELECT_STRING.length
      )
  }

  /**
    * Left trim the incoming string
    *
    * @param s -> incoming string
    * @return
    */
  def ltrim(s: String): String = s.replaceAll("^\\s+", "")

  /**
    * Validates the SQL, if its of execute query pattern
    *
    * @param sql -> Incoming query
    * @return
    */
  def isQueryOfGivenSeqType(sql: String,
                            queryTypes: Seq[String] =
                              QueryConstants.IS_EXECUTE_QUERY): Boolean = {
    var containsExecuteQuery: Boolean = false
    breakable {
      for (executeQueryConstant <- queryTypes) {
        if (QueryParserUtils
              .ltrim(sql)
              .regionMatches(
                true,
                0,
                executeQueryConstant,
                0,
                executeQueryConstant.length
              )) {
          containsExecuteQuery = true
          break
        }
      }
    }
    containsExecuteQuery
  }

  /**
    * Checks if the incoming Explain plan has any sample rows available in it
    *
    * @param explainPlan -> Explain plan generated per the SQL submitted
    * @return
    */
  def isHavingSampleRows(explainPlan: String): Boolean = {
    QueryConstants.SAMPLE_ROWS_PATTERN
      .findAllIn(explainPlan.toLowerCase)
      .nonEmpty
  }

  /**
    * Checks if a Query has any specific analytical function like GROUP BY, COUNT, HAVING, OVER,
    * ORDER BY, SUM, MIN, MAX, SUM, FIRST_VALUE, LAST_VALUE,
    * ROW_NUMBER, RANK, DENSE_RANK ..
    *
    * @param sql SQL String
    * @return true - if there is any "analytical function" clause, else false
    */
  def isHavingAnalyticalFunction(sql: String): Boolean = {
    def MethodName: String =
      new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val uniformSQL = sql
      .replace(QueryConstants.NEW_LINE, QueryConstants.SPACE_CHAR)
      .toUpperCase()
    import scala.util.control.Breaks._
    var containsAnalyticalFunction: Boolean = false
    breakable {
      for (function <- QueryConstants.TERADATA_ANALYTICAL_FUNCTIONS) {
        val pattern = s"\\b$function\\b".r
        val iterator = pattern.findAllIn(uniformSQL)
        if (iterator.nonEmpty) {
          containsAnalyticalFunction = true
          break
        }
      }
    }
    containsAnalyticalFunction
  }

  /**
    * Checks if a Query has Select clause
    *
    * @param sql SQL String
    * @return true - if there is an "insert" clause, else false
    */
  def isHavingSelect(sql: String): Boolean = {
    def MethodName: String =
      new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)
    val uniformSQL = sql.replace("\n", " ")
    uniformSQL.toUpperCase().contains(QueryConstants.SQL_SELECT_STRING)
  }

  /**
    * Gets all Source Tables from a Query String
    *
    * @param sql        SQL String
    * @param searchList List[String] ->  inorder to get from all types of SQL pass searchlist
    *                   to be List("into", "view", "table", "from", "join")
    * @return Seq[Tables]
    */
  def getAllSourceTables(
    sql: String,
    searchList: Seq[SearchCriteria] = SearchSchemaUtils.ALL_TABLES_SEARCH_CRITERIA
  ): Seq[String] = {

    val allIdentifiableTablesANSIType: scala.Seq[_root_.scala.Predef.String] = getTables(sql, searchList)
    val nonANSIJoinTables: Seq[String] =
      Try(getSourceTablesFromNonAnsi(sql)).getOrElse(Seq.empty)
    val finalList =
      (nonANSIJoinTables ++ allIdentifiableTablesANSIType).distinct
        .map(_.toLowerCase())
    logger.info(
      s"Source Tables from Non-ANSI Join --> ${nonANSIJoinTables.mkString("[", " , ", "]")}"
    )
    logger.info(
      s"Source Tables from ANSI type Join --> ${allIdentifiableTablesANSIType
        .mkString("[", " , ", "]")}"
    )
    logger.info(
      s"Final List of Tables --> ${finalList.mkString("[", " , ", "]")}"
    )
    finalList
  }

  private def getTables(sql: String, searchCriterias: Seq[SearchCriteria]): Seq[String] = {
    val tokens: Array[String] = tokenize(sql)
    val tokenAndStuff: Map[Int, (String, Boolean)] = SearchSchemaUtils.matchTables(tokens, searchCriterias)
    // stripping suffix for cases such as
    // (select * from tbl) - where table is returned as 'tbl)'
    val allIdentifiableTablesANSIType: Seq[String] = pickSearchTokens(tokenAndStuff).map { x =>
      x.stripSuffix(")")
    }
    allIdentifiableTablesANSIType
  }

  def removeSQLComments(sql: String): String = sql.split(QueryConstants.NEW_LINE)
    .filter(!_.startsWith(QueryConstants.SQL_COMMENT))
    .mkString(QueryConstants.NEW_LINE)

  /**
    * Gets the final list of Source Tables from a list of Token
    *
    * @param tokenAndStuff Example : Map( 0 -> (select, false), 1 -> (*,false), 2 -> (from,true), 3 -> (tmp,false))
    * @return Example : Seq(tmp)
    */
  def pickSearchTokens(
    tokenAndStuff: Map[Int, (String, Boolean)]
  ): Seq[String] = {
    var finalList = new ListBuffer[String]()
    tokenAndStuff.foreach { each =>
      val (index, (_, isMet)) = each
      // Ensure pick criteria is met
      // Also ensure next token is not staring with a '('
      if (isMet && !tokenAndStuff(index + 1)._1.startsWith("(")) {
        finalList += tokenAndStuff(index + 1)._1
      }
    }
    finalList.toList
  }

  /**
    * Fetches a list of tables that are in a join clause, but the join clause is non-ANSI format.
    *
    * @param sql SQL String
    * @return List (tables)
    */
  def getSourceTablesFromNonAnsi(sql: String): Seq[String] = {
    def MethodName: String =
      new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val tokensAsMap1: Map[Int, (String, Boolean)] =
      tokensWithPickings(tokenize(sql).toSeq, Set("FROM"))
    if (SQLNonANSIJoinParser.isSQLNonANSIJoin(sql)) {
      SQLNonANSIJoinParser.getTablesFromTokensAsMapNonANSI(tokensAsMap1)
    } else {
      Seq()
    }
  }

  /**
    * Checks whether a supplied set of search Criteria is matching the list of tokens
    *
    * @param tokens       List of Tokens | Example : List (select, *, from, tmp )
    * @param pickCriteria Set of search strings | Example : Set ( from, join )
    * @return Example : Map( 0 -> (select, false), 1 -> (*,false), 2 -> (from,true), 3 -> (tmp,false) )
    */
  def tokensWithPickings(
    tokens: Seq[String],
    pickCriteria: Set[String]
  ): Map[Int, (String, Boolean)] = {
    val tokensWithIndex: Map[Int, String] = QueryParserUtils.tokensAsMap(tokens)
    tokensWithIndex.map { eachEntry =>
      // As pickCriteria is a set with upper case search list directly validating contains
      (
        eachEntry._1,
        (eachEntry._2, pickCriteria contains (eachEntry._2.toUpperCase))
      )
    }
  }

  /**
    * Breaks each token in the SQL into a Map
    * select * from tmp --> Map( 0 -> select, 1 -> *, 2 -> from, 3 -> tmp)
    *
    * @param tokens List of Tokens | Example : List (select, *, from , tmp)
    * @return Map[Index, Token] | Example : Map( 0 -> select, 1 -> *, 2 -> from, 3 -> tmp)
    */
  def tokensAsMap(tokens: Seq[String]): Map[Int, String] = {
    var index = 0
    tokens.map { x =>
      val s = (index, x)
      index += 1
      s
    }
  }.toMap

  /**
    * Helper function to call a function which is recursive to get the Target table names from the AST
    *
    * @param sql to be parsed
    * @return - List of target tables if any. If it is select only table, it returns a None.
    */

  def getTargetTables(sql: String): Seq[String] =
    getTables(sql, searchCriterias = SearchSchemaUtils.TARGET_TABLES_SEARCH_CRITERIA)



  /**
    *
    * @param sql -> sql to be validated
    * @param isDropTableAsTempTable -> Indicating if the incoming SQL has tables which are temp / cache table
    * @return
    */
  def isDDL(sql: String, isDropTableAsTempTable: Boolean = false): Boolean = {
    val tokenized = tokenize(sql)
    val nonEmptyStrTokenized = tokenized.filter(x => !x.isEmpty)
    nonEmptyStrTokenized.head.toUpperCase.contains(QueryConstants.DDL_CREATE_STRING) ||
      nonEmptyStrTokenized.head.toUpperCase.contains(QueryConstants.DDL_DROP_STRING) && !isDropTableAsTempTable ||
      nonEmptyStrTokenized.head.toUpperCase.contains(QueryConstants.DDL_TRUNCATE_STRING) ||
      nonEmptyStrTokenized.head.toUpperCase.contains(QueryConstants.DDL_DELETE_STRING)
  }

  /**
    * Checks if a Query has Insert or if its just a select
    *
    * @param sql SQL String
    * @return true - if there is an "insert" clause, else false
    */
  def isHavingInsert(sql: String): Boolean = {
    tokenize(sql).head.equalsIgnoreCase("insert")
  }

  /**
    * Parse the SQL and get the entire select clause
    *
    * @param sql SQL String
    * @return SQL String - that has just the select clause
    */
  def getSelectClause(sql: String): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val uniformSQL = sql.replace("\n", " ")
    val sqlParts: Array[String] = uniformSQL.split(" ")
    val selectClauseOnly = if (isHavingInsert(sql)) {
      val index = sqlParts.indexWhere(_.toUpperCase() == "SELECT")
      sqlParts.slice(index, sqlParts.length).mkString(" ")
    } else {
      sqlParts.mkString(" ")
    }
    selectClauseOnly
  }

  /**
    *
    * @param multiLineSql -> Incoming SQL
    * @return
    */
  def getMultiLineSqlStatements(multiLineSql: String): Array[String] = {
    val uniformSQL = multiLineSql.replace("\n", " ").trim
    val sqlArray: Array[String] = uniformSQL.split(QueryConstants.SEMI_COLON)
    sqlArray
  }

  /**
    * Checks if a Query has Limit caluse
    *
    * @param sql SQL String
    * @return true - if there is an "limit" clause, else false
    */
  def isHavingLimit(sql: String): Boolean = {
    def MethodName: String =
      new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)
    val uniformSQL = sql.replace("\n", " ")
    uniformSQL.toUpperCase().contains(QueryConstants.SQL_LIMIT_STRING)
  }

  /**
    * Parse the SQL and get the limit if specified
    *
    * @param sql -> Incoming SQL
    * @return limit specified in SQL
    */
  def getLimit(sql: String): Int = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)
    val tokens = tokenize(sql)
    val tokensAsMap = tokensWithPickings(tokens, Set("LIMIT"))
    tokensAsMap.toSeq.foreach { tokenMap =>
      val token = tokenMap._2._1
      val tokenIndex = tokenMap._1
      if (token.equalsIgnoreCase("LIMIT") && (tokenIndex + 1) < tokens.length) {
          return tokensAsMap(tokenIndex + 1)._1.toInt
      }
    }
    throw new SQLException("Invalid SQL - No limit specified with limit clause.")
  }
}
