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

package com.paypal.gimel.sql.livy

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql.SparkSession
import spray.json._

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.logger.Logger

object LivyGimelWrapper {

  val logger: Logger = Logger(this.getClass.getName)
  val livyClient = new LivyInteractiveClient()

  /**
    * Livy query execution wrapper which checks whether livy session exists or not.
    * if exists, execute the sql statement through a livy session if not create session and executes it
    *
    * @param sql          - incoming sql
    * @param currentUser  - the proxy user
    * @param sparkSession - spark session
    * @return - a pair with boolean of success and failure and error string
    */
  def execute(sql: String, currentUser: String, sparkSession: SparkSession): (Boolean, String) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    val sessionID = sparkSession.sparkContext.getLocalProperty(GimelConstants.GTS_GIMEL_LIVY_SESSION_ID)
    val livyEndPoint = sparkSession.conf.get(GimelConstants.LIVY_SPARK_ENDPOINT_KEY)
    val livyGimelJars = sparkSession.conf.get(GimelConstants.LIVY_SPARK_GIMEL_JARS)

    logger.info(" @Begin --> " + MethodName)
    logger.info("isSessionIdle to be called")
    isSessionIdle(currentUser, sessionID, livyEndPoint) match {
      case true =>
        executeStatement(sql, currentUser, sessionID, livyEndPoint)
      case false => {
        logger.info("start session going to be called")
        startSession(currentUser, sessionID, livyEndPoint, livyGimelJars, sparkSession)
        var timeCounterSec = 0
        val sleepTimeMs = 1000
        val timeOutSec = sparkSession.conf.get(GimelConstants.GTS_DDL_TIMEOUT_MILLISEC, "300").toInt
        while (!(isSessionIdle(currentUser, sessionID, livyEndPoint))) {
          logger.info(s"Sleeping MilliSec [${sleepTimeMs}] to get idle session...")
          timeCounterSec += 1
          if (timeCounterSec >= timeOutSec) {
            stopSession(currentUser, sessionID, livyEndPoint, livyGimelJars, sparkSession)
            throw new Exception(
              s"""
                 |The DDL operation required starting a new Spark Session.
                 |However, attempt to acquire a new session timed out after default [${timeOutSec}] seconds.
                 |If you'd like to wait longer/shorter (say 600/120 secs), then set following configuration :
                 |set gimel.gts.ddl.session.creation.timeout.sec=600
              """.stripMargin
            )
          }
          Thread.sleep(sleepTimeMs)
        }
        logger.info("calling execute statement")
        val confSql = sparkSession.conf.getAll.map{x => s"set ${x._1}=${x._2}"}.mkString(";")
        executeStatement(confSql, currentUser, sessionID, livyEndPoint)
        executeStatement(sql, currentUser, sessionID, livyEndPoint)
      }
    }
  }

  /**
    * This function starts a livy session for the given user
    *
    * @param currentUser             - incoming user
    * @param sessionID               - session id
    * @param livyEndPoint            - Livy host address and port
    * @param sparkSession            - SparkSession
    */
  def startSession(currentUser: String, sessionID: String, livyEndPoint: String, livyGimelJars: String, sparkSession: SparkSession) : Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val currentSession = currentUser + GimelConstants.GTS_LIVY_DML_SESSION_KEYWORD
    val confMap = getConfMapFromSparkSession(sparkSession)

    val startMap = Map(GimelConstants.LIVY_PARAM_START_SESSION_KEY -> GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_TRUE_VALUE,
      GimelConstants.LIVY_PARAM_APPLICATION_NAME_KEY -> currentSession,
      GimelConstants.LIVY_PARAM_APPLICATION_KIND_KEY -> GimelConstants.LIVY_PARAM_APPLICATION_KIND_SCALA_VALUE,
      GimelConstants.LIVY_PARAM_PROXY_USER_KEY -> currentUser,
      GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_KEY -> GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_TRUE_VALUE,
      GimelConstants.LIVY_PARAM_JARS_KEY -> livyGimelJars,
      GimelConstants.LIVY_PARAM_CLUSTER_KEY -> livyEndPoint,
      GimelConstants.LIVY_PARAM_CONF_KEY -> confMap).map(x => (Symbol(x._1), x._2))
    livyClient.startSession(startMap)
  }

  /**
    * This function stops a livy session for the given user
    *
    * @param currentUser             - incoming user
    * @param sessionID               - session id
    * @param livyEndPoint            - Livy host address and port
    * @param sparkSession           -  SparkSession
    */
  def stopSession(currentUser: String, sessionID: String, livyEndPoint: String, livyGimelJars: String, sparkSession: SparkSession): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val currentSession = currentUser + GimelConstants.GTS_LIVY_DML_SESSION_KEYWORD
    val confMap = getConfMapFromSparkSession(sparkSession)

    val startMap = Map(GimelConstants.LIVY_PARAM_START_SESSION_KEY -> GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_TRUE_VALUE,
      GimelConstants.LIVY_PARAM_APPLICATION_NAME_KEY -> currentSession,
      GimelConstants.LIVY_PARAM_APPLICATION_KIND_KEY -> GimelConstants.LIVY_PARAM_APPLICATION_KIND_SCALA_VALUE,
      GimelConstants.LIVY_PARAM_PROXY_USER_KEY -> currentUser,
      GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_KEY -> GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_TRUE_VALUE,
      GimelConstants.LIVY_PARAM_JARS_KEY -> livyGimelJars,
      GimelConstants.LIVY_PARAM_CLUSTER_KEY -> livyEndPoint,
      GimelConstants.LIVY_PARAM_CONF_KEY -> confMap).map(x => (Symbol(x._1), x._2))
    livyClient.stopSession(startMap)
  }

  /**
    * Executes a SQL statement in the current user's livy session
    *
    * @param sql          - user supplied SQL
    * @param currentUser  - the current user
    * @param sessionID    - Session ID of Livy
    * @param livyEndPoint - Livy host end point
    * @return - a pair with boolean of success and failure and error string
    */
  def executeStatement(sql: String, currentUser: String, sessionID: String, livyEndPoint: String): (Boolean, String) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val trimmedSql = sql.replaceAll("[\n\r]", " ")
    val escapedString = StringEscapeUtils.escapeJava(trimmedSql)
    val gimelGSQLWrapperCode = s"""com.paypal.gimel.scaas.GimelQueryProcessor.executeBatch("${escapedString}", spark)"""
    val currentSession = currentUser + GimelConstants.GTS_LIVY_DML_SESSION_KEYWORD
    val executeMap: Map[Symbol, String] = Map(GimelConstants.LIVY_PARAM_WAIT_SESSION_KEY -> GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_TRUE_VALUE,
      GimelConstants.LIVY_PARAM_APPLICATION_NAME_KEY -> currentSession,
      GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_KEY -> GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_TRUE_VALUE,
      GimelConstants.LIVY_PARAM_CODE_KEY -> gimelGSQLWrapperCode,
      GimelConstants.LIVY_PARAM_CLUSTER_KEY -> livyEndPoint,
      GimelConstants.LIVY_PARAM_STATEMENTS_KEY -> GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_TRUE_VALUE).map(x => (Symbol(x._1), x._2))

    val statementStatus = livyClient.executeStatement(executeMap)
    val StateMentMap = Map(GimelConstants.LIVY_PARAM_APPLICATION_NAME_KEY -> currentSession, GimelConstants.LIVY_PARAM_CLUSTER_KEY -> livyEndPoint).map(x => (Symbol(x._1), x._2))

    // After the statement gets executed, get the statement ID and use it to query the status
    // we return success if the response from Livy is "ok" else we catch the error in traceback JSON key and send it to the caller
    val statementResult: String = livyClient.getStatementsLog(StateMentMap, statementStatus.get.id)
    statementResult.parseJson.asJsObject.fields.get("output").toList(0).asJsObject.fields.get("status").get.toString match {
      case "\"ok\"" => {
        (true, "SUCCESS")
      }
      case _ => {
        (false, statementResult.parseJson.asJsObject.fields.get("output").toList(0).asJsObject.fields.get("traceback").get.toString)
      }
    }
  }

  /**
    * This function checks whether the session held for the user supplied is in idle status
    *
    * @param currentUser  - Incoming user
    * @param sessionID    - Livy Session ID
    * @param livyEndPoint - Livy interactive End point
    * @return - Boolean which tells whether session exists and idle or not
    */
  def isSessionIdle(currentUser: String, sessionID: String, livyEndPoint: String): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val currentSession = currentUser + GimelConstants.GTS_LIVY_DML_SESSION_KEYWORD
    val listMap = Map("list" -> "true",
      GimelConstants.LIVY_PARAM_APPLICATION_NAME_KEY -> currentSession,
      GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_KEY -> GimelConstants.LIVY_PARAM_INTERACTIVE_JOB_TRUE_VALUE,
      GimelConstants.LIVY_PARAM_CLUSTER_KEY -> livyEndPoint).map(x => (Symbol(x._1), x._2))
    val getUrl = "http://%s/sessions/%s".format(listMap(Symbol(LivyClientArgumentKeys.Cluster)), currentSession)
    val state: Option[State] = LivyInteractiveJobAction.getSession(getUrl, livyClient.userCredentials(listMap), false)
    state match {
      case None => false
      case _ => {
        state.get.state match {
          case GimelConstants.LIVY_STATUS_IDLE => true
          case _ => false
        }
      }
    }
  }

  def getConfMapFromSparkSession(sparkSession: SparkSession): Map[String, String] = {
    val livySparkConfListString = sparkSession.conf.get(GimelConstants.LIVY_SPARK_CONF, "")
    val livySparkConfList = livySparkConfListString.split(",")
    val confMap = livySparkConfList.map(eachConf => {
      (eachConf, sparkSession.conf.get(eachConf, ""))
    }).filter(_._2 != "").toMap
    logger.info("Starting/Stopping livy session with spark conf --> " + confMap)
    val livyGimelExtraClassPath = sparkSession.conf.get(GimelConstants.LIVY_GIMEL_EXTRA_CLASS_PATH)
    confMap ++ Map(GimelConstants.LIVY_PARAM_SPARK_DRIVER_EXTRA_CLASS_PATH_KEY -> livyGimelExtraClassPath,
      GimelConstants.LIVY_PARAM_SPARK_EXECUTOR_EXTRA_CLASS_PATH_KEY -> livyGimelExtraClassPath)
  }
}
