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

import org.slf4j.{Logger, LoggerFactory}

object SQLNonANSIJoinParser {

  private val logger: Logger = LoggerFactory.getLogger(QueryParserUtils.getClass)

  /**
    * Determine if non-ANSI standard join (orcale like) is in the SQL
    *
    * @param tokenAndStuff Tokens as Map
    * @param startPoint    Index position from where to look in the collection of tokens
    * @return True / False
    */
  def checkIfNonANSIJoin(tokenAndStuff: Map[Int, (String, Boolean)], startPoint: Int): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val length = tokenAndStuff.size
    val indexOfFrom = startPoint + 1
    val hasNonAnsiJoin = if ((length >= indexOfFrom + 2)) {
      (tokenAndStuff(indexOfFrom + 1)._1 == "," || tokenAndStuff(indexOfFrom + 2)._1 == ",")
    } else {
      false
    }
    hasNonAnsiJoin
  }

  /**
    * Given a SQL - determine if non-ANSI standard join (oracle like) is in the SQL
    *
    * @param sql SQL String
    * @return True / False
    */
  def isSQLNonANSIJoin(sql: String): Boolean = {
    val pickings: Map[Int, (String, Boolean)] =
      QueryParserUtils.tokensWithPickings(QueryParserUtils.tokenize(sql).toSeq, Set("FROM"))
    val startPoints: Iterable[Int] = pickings.filter(x => x._2._2).keys.toSeq
    startPoints.exists { eachKey =>
      checkIfNonANSIJoin(pickings, eachKey)
    }
  }

  /**
    * Fetches a list of tables that are in a join clause, but the join clause is non-ANSI format.
    *
    * @param tokensAsMap Tokens as Map with Index & Search Criteria Met Flag
    * @return List(tables)
    */
  def getTablesFromTokensAsMapNonANSI(tokensAsMap: Map[Int, (String, Boolean)]): Seq[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    var fromReached = false
    var tables = Seq[String]()
    tokensAsMap.toSeq.sortBy(x => x._1).foreach { tokenMap =>
      val token = tokenMap._2._1
      val tokenIndex = tokenMap._1
      if (token.equalsIgnoreCase("from")) fromReached = true
      else {
        if (fromReached) {
          if (token == ",") {
            //        println("inside", token, tokenIndex, tokensAsMap1(tokenIndex - 3), tokensAsMap1(tokenIndex - 2), tokensAsMap1(tokenIndex - 1))
            if (tokensAsMap(tokenIndex - 3)._1 == "," || tokensAsMap(tokenIndex - 3)._1.equalsIgnoreCase("from")) {
              tables ++= Seq(tokensAsMap(tokenIndex - 2)._1)
            }
            if (tokensAsMap(tokenIndex - 2)._1 == "," || tokensAsMap(tokenIndex - 2)._1.equalsIgnoreCase("from")) {
              tables ++= Seq(tokensAsMap(tokenIndex - 1)._1)
            }
          }
          else {
            //        println("non-comma", token, tokenIndex)
            //        println("end?", isEndCriteria(token) || tokenIndex == tokensAsMap1.size - 1)
            if (isEndCriteriaNonAnsi(token)) {
              //            println("isEndCriteria")
              if (tokenIndex == tokensAsMap.size - 1) {
                // a k,b l, c l, d k )
                //              println("last token", tokensAsMap1(tokenIndex - 1)._1, tokensAsMap1(tokenIndex - 2)._1, tokensAsMap1(tokenIndex - 3)._1)
                if (tokensAsMap(tokenIndex - 2)._1 == "," || tokensAsMap(tokenIndex - 2)._1.equalsIgnoreCase("from")) {
                  tables ++= Seq(tokensAsMap(tokenIndex - 1)._1)
                }
                if (tokensAsMap(tokenIndex - 3)._1 == "," || tokensAsMap(tokenIndex - 3)._1.equalsIgnoreCase("from")) {
                  tables ++= Seq(tokensAsMap(tokenIndex - 2)._1)
                }
              }
              else {
                // a k,b l, c l, d k ) t1
                //              println(s"not last token ${token}")
                if (tokensAsMap(tokenIndex - 2)._1 == "," || tokensAsMap(tokenIndex - 2)._1.equalsIgnoreCase("from")) {
                  tables ++= Seq(tokensAsMap(tokenIndex - 1)._1)
                }
                if (tokensAsMap(tokenIndex - 3)._1 == "," || tokensAsMap(tokenIndex - 3)._1.equalsIgnoreCase("from")) {
                  tables ++= Seq(tokensAsMap(tokenIndex - 2)._1)
                }
              }
              fromReached = false
            }
            else {
              // a k,b l, c l, d k
              if (tokenIndex == tokensAsMap.size - 1) {
                //              println("LAST")
                if (tokensAsMap(tokenIndex - 1)._1 == "," || tokensAsMap(tokenIndex - 1)._1.equalsIgnoreCase("from")) {
                  tables ++= Seq(tokensAsMap(tokenIndex)._1)
                }
                if (tokensAsMap(tokenIndex - 2)._1 == "," || tokensAsMap(tokenIndex - 2)._1.equalsIgnoreCase("from")) {
                  tables ++= Seq(tokensAsMap(tokenIndex - 1)._1)
                }
                fromReached = false
              }
              else {
                //              println("uncaught1", tokenIndex, token)
                // not an end
              }
            }
          }
        }
        else {
          //        println("uncaught2", tokenIndex, token)
          //  not an end
        }
      }

    }
    tables
  }

  /**
    * Fetches a list of tables that are in a join clause, but the join clause is non-ANSI format.
    *
    * @param sql SQL String
    * @return List (tables)
    */
  def getSourceTablesFromNonAnsi(sql: String): Seq[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val tokensAsMap1: Map[Int, (String, Boolean)] =
      QueryParserUtils.tokensWithPickings(QueryParserUtils.tokenize(sql).toSeq, Set("FROM"))
    if (isSQLNonANSIJoin(sql)) getTablesFromTokensAsMapNonANSI(tokensAsMap1)
    else Seq()
  }

  /**
    * Utility method to determine the end pattern exists
    *
    * @param token Token to Search
    * @return True / False
    * @example isEndCriteria(")") = true | isEndCriteria("abc") = false
    */
  def isEndCriteriaNonAnsi(token: String): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    val met = token.toLowerCase() match {
      case ";" | ")" | "where" | "union" => true
      case _ => false
    }
    //  println("met?", token, met)
    met
  }

}
