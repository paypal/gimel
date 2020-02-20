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

import scala.collection.immutable.Map

object SearchSchemaUtils {
  val ALL_TABLES_SEARCH_CRITERIA: Seq[SearchCriteria] = Seq(
    From,
    InsertInto,
    View,
    TableIfExists,
    TableIfNotExists,
    Table,
    Join,
    Describe,
    DeleteAll,
    DeleteTableAll,
    CreateProcedure,
    Call,
    Execute,
    CollectStatisticsOn
  )

  val SOURCE_TABLES_SEARCH_CRITERIA: Seq[SearchCriteria] = Seq(
    From,
    Join
  )

  // LIMITING only to insert commands
  val TARGET_TABLES_SEARCH_CRITERIA: Seq[SearchCriteria] = Seq(
    InsertInto,
    Table
  )

  def matchTables(tokens: Array[String],
                  searchCriterias: Seq[SearchCriteria] = ALL_TABLES_SEARCH_CRITERIA): Map[Int, (String, Boolean)] = {
    val tokensWithIndex: Map[Int, String] =
      QueryParserUtils.tokensAsMap(tokens)
    tokensWithIndex.map { eachEntry =>
      (
        eachEntry._1,
        (
          eachEntry._2,
          searchCriterias.exists(
            searchCriteria =>
              searchCriteria.matchCriteria(eachEntry._1, tokensWithIndex)
          )
        )
      )
    }
  }

  def validateToken(currentTokenKey: Int,
                    tokens: Map[Int, String],
                    matchCriteria: Seq[String]): Boolean = {
    if (tokenContainsKey(currentTokenKey, matchCriteria.size, tokens)) {
      tokenContainsValue(currentTokenKey, matchCriteria, tokens)
    } else {
      false
    }
  }

  def tokenContainsKey(currentTokenKey: Int,
                       size: Int,
                       tokens: Map[Int, String]): Boolean = {
    (((currentTokenKey - size) + 1) to currentTokenKey)
      .forall(key => tokens.contains(key))
  }

  def tokenContainsValue(currentTokenKey: Int,
                         matchCriteria: Seq[String],
                         tokens: Map[Int, String]): Boolean = {
    var criteriaCounter = 0
    (((currentTokenKey - matchCriteria.size) + 1) to currentTokenKey)
      .forall(key => {
        val criteriaMatch =
          tokens(key).equalsIgnoreCase(matchCriteria(criteriaCounter))
        if (criteriaMatch) {
          criteriaCounter += 1
          criteriaMatch
        } else {
          false
        }
      })
  }
}

sealed trait SearchCriteria {
  def matchCriteria(currentTokenKey: Int, tokens: Map[Int, String]): Boolean =
    false
}

object From extends SearchCriteria {
  private val criteria: Seq[String] = Seq("FROM")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }

}

object InsertInto extends SearchCriteria {
  private val criteria: Seq[String] = Seq("INSERT", "INTO")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object View extends SearchCriteria {
  private val criteria: Seq[String] = Seq("VIEW")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object Table extends SearchCriteria {
  private val criteria: Seq[String] = Seq("TABLE")
  private val IF: String = "IF"

  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria) && tokens
      .contains(currentTokenKey + 1) && !tokens(currentTokenKey + 1).trim
      .equalsIgnoreCase(IF)
  }
}

object Join extends SearchCriteria {
  private val criteria: Seq[String] = Seq("JOIN")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object Describe extends SearchCriteria {
  private val criteria: Seq[String] = Seq("DESCRIBE")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object DeleteAll extends SearchCriteria {
  // DELETE ALL Forgetcode.emp
  private val criteria: Seq[String] = Seq("DELETE", "ALL")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object DeleteTableAll extends SearchCriteria {
  // DELETE Forgetcode.emp ALL
  private val DELETE: String = "DELETE"
  private val ALL: String = "ALL"
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    if (tokens.contains(currentTokenKey) && tokens.contains(
          currentTokenKey + 2
        )) {
      tokens(currentTokenKey).equalsIgnoreCase(DELETE) && tokens(
        currentTokenKey + 2
      ).equalsIgnoreCase(ALL)
    } else {
      false
    }
  }
}

object TableIfExists extends SearchCriteria {
  // table if exists
  private val criteria: Seq[String] = Seq("TABLE", "IF", "EXISTS")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object TableIfNotExists extends SearchCriteria {
  // table if not exists
  private val criteria: Seq[String] = Seq("TABLE", "IF", "NOT", "EXISTS")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object CreateProcedure extends SearchCriteria {
  private val criteria: Seq[String] = Seq("CREATE", "PROCEDURE")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object Call extends SearchCriteria {
  private val criteria: Seq[String] = Seq("CALL")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object Execute extends SearchCriteria {
  private val criteria: Seq[String] = Seq("EXECUTE")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}

object CollectStatisticsOn extends SearchCriteria {
  private val criteria: Seq[String] = Seq("COLLECT", "STATISTICS", "ON")
  override def matchCriteria(currentTokenKey: Int,
                             tokens: Map[Int, String]): Boolean = {
    SearchSchemaUtils.validateToken(currentTokenKey, tokens, criteria)
  }
}
