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

package com.paypal.gimel.common.utilities

object SQLTagUtils {

  private val SQL_CLOSED_BRACKET = ')'

  import GenericUtils._
  import SQLDataTypesUtils._

  def tagByWhereClause(sql: String): SQLTag = {
    val lastIndexOfWhere = sql.lastIndexOf(WHERE)
    if (containsWord(sql, WHERE_PATTERN) && lastIndexOfWhere != -1) {
      if (sql.length <= lastIndexOfWhere + WHERE.length + 1) {
        throw new IllegalStateException(
          s"SQL received for tagging[$sql] is not a valid SQL"
        )
      }
      val sqlPartToBeValidated =
        sql.substring(lastIndexOfWhere + WHERE.length + 1)
      if (GenericUtils.hasValidBraces(sqlPartToBeValidated)) {
        getMatchingTag(
          getSqlStringAfterSubQuery(sqlPartToBeValidated),
          isWhereClauseAvailable = true
        )
      } else {
        // Since the lastIndex of "Where" is having a non closed bracket, its part of some sub query
        getMatchingTag(
          getSqlStringAfterSubQuery(sqlPartToBeValidated),
          isWhereClauseAvailable = false
        )
      }
    } else {
      getMatchingTag(sql, isWhereClauseAvailable = false)
    }
  }

  private def getMatchingTag(sqlPartToBeValidated: String,
                             isWhereClauseAvailable: Boolean): SQLTag = {
    sqlPartToBeValidated match {
      case _
          if containsWord(sqlPartToBeValidated, GROUP_BY_PATTERN) && isWhereClauseAvailable =>
        WhereWithGroupBy
      case _
          if containsWord(sqlPartToBeValidated, ORDER_BY_PATTERN) && isWhereClauseAvailable =>
        WhereWithOrderBy
      case _ if containsLimit(sqlPartToBeValidated) && isWhereClauseAvailable =>
        WhereWithLimit
      case _ if containsWord(sqlPartToBeValidated, GROUP_BY_PATTERN) =>
        GroupBy
      case _ if containsWord(sqlPartToBeValidated, ORDER_BY_PATTERN) =>
        OrderBy
      case _ if containsLimit(sqlPartToBeValidated) =>
        Limit
      case _ if isWhereClauseAvailable =>
        Where
      case _ =>
        WithoutWhere
    }
  }

  def containsLimit(sql: String): Boolean = {
    containsWord(sql, SAMPLE_PATTERN) || containsWord(sql, LIMIT_PATTERN)
  }

  def getSqlStringAfterSubQuery(sql: String): String = {
    val lastIndexOfClosedBracket =
      sql.lastIndexOf(SQL_CLOSED_BRACKET)
    if (lastIndexOfClosedBracket != -1) {
      sql.substring(lastIndexOfClosedBracket)
    } else {
      sql
    }
  }
}

sealed trait SQLTag

object Where extends SQLTag
object WithoutWhere extends SQLTag
object WhereWithGroupBy extends SQLTag
object WhereWithOrderBy extends SQLTag
object WhereWithLimit extends SQLTag
object GroupBy extends SQLTag
object OrderBy extends SQLTag
object Limit extends SQLTag
