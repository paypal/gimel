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

import scala.util.matching.Regex

object QueryConstants {

  val DDL_CREATE_STRING: String = "CREATE"
  val DDL_DROP_STRING: String = "DROP"
  val DDL_TRUNCATE_STRING: String = "TRUNCATE"
  val DDL_ALTER_STRING: String = "ALTER"
  val DDL_FROM_STRING: String = "FROM"
  val DDL_DELETE_STRING: String = "DELETE"
  val DML_INSERT_STRING: String = "INSERT"
  val DML_UPDATE_STRING: String = "UPDATE"
  val SQL_SELECT_STRING: String = "SEL"
  val SQL_LIMIT_STRING: String = "LIMIT"
  val IS_EXECUTE_QUERY: Seq[String] = Seq(SQL_SELECT_STRING, "EXPLAIN")
  val IS_DDL_QUERY: Seq[String] = Seq(DDL_CREATE_STRING, DDL_DROP_STRING, DDL_TRUNCATE_STRING, DDL_ALTER_STRING)
  val IS_DDL_DML_QUERY: Seq[String] = IS_DDL_QUERY ++ Seq(
    DDL_DELETE_STRING, DML_INSERT_STRING, DML_UPDATE_STRING, "COLLECT"
  )
  val SAMPLE_ROWS_IDENTIFIER: String = "samples are specified as a number of rows"
  val SAMPLE_ROWS_PATTERN: Regex = s"\\b$SAMPLE_ROWS_IDENTIFIER\\b".r
  val NEW_LINE: String = "\n"
  val SPACE_CHAR: String = " "
  val SQL_COMMENT : String = "--"
  val SEMI_COLON: String = ";"
  val TERADATA_ANALYTICAL_FUNCTIONS: Seq[String] = Seq("GROUP", "COUNT", "HAVING", "OVER",
    "ORDER", "SUM", "MIN", "MAX", "SUM", "FIRST_VALUE", "LAST_VALUE", "ROW_NUMBER", "RANK", "DENSE_RANK")
  val GTS_GET_ALL_TABLES_DEFAULT_SEARCH_LIST: Set[String] = Set("FROM", "JOIN")
  val GTS_GET_ALL_TABLES_ALL_SQL_TYPES_SEARCH_LIST: Set[String] = Set("INTO", "VIEW", "TABLE", "FROM",
    "JOIN", "DESCRIBE")
}
