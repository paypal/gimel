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

class LivyInteractiveClient extends JobClient {

  /**
    * List of sessions are printed
    * @param inputOptions - list of input options
    */
  def list(inputOptions: LivyClientArgumentParser.ArgumentMap): Unit = {

    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))
    val sessionName: Option[String] = inputOptions.get(Symbol(LivyClientArgumentKeys.ApplicationName)).asInstanceOf[Option[String]]

    sessionName match {
      case None =>
        val listURL = "http://%s/sessions".format(inputOptions(Symbol(LivyClientArgumentKeys.Cluster)))
        LivyInteractiveJobAction.list(listURL, userCredentials(inputOptions), testMode)
      case Some(session) =>
        println(session)
        val getUrl = "http://%s/sessions/%s".format(inputOptions(Symbol(LivyClientArgumentKeys.Cluster)), session)
        val dd: Option[State] = LivyInteractiveJobAction.getSession(getUrl, userCredentials(inputOptions), testMode)
        println(dd)
    }
  }

  /**
    * Starts a livy session. It monitors the session creation by querying the Livy to check whether the session got created
    * @param inputOptions - list of input options
    */
  def startSession(inputOptions: LivyClientArgumentParser.ArgumentMap): Unit = {
    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))

    val startSessionUrl = "http://%s/sessions".format(inputOptions(Symbol(LivyClientArgumentKeys.Cluster)))

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.StartSession))) {
      val sessionRequest: SessionRequest = SessionRequest(
        inputOptions(Symbol(LivyClientArgumentKeys.ProxyUser)).asInstanceOf[String],
        inputOptions.getOrElse(Symbol(LivyClientArgumentKeys.Kind), "spark").asInstanceOf[String],
        inputOptions.getOrElse(Symbol(LivyClientArgumentKeys.ApplicationName), "livy").asInstanceOf[String],
        inputOptions.getOrElse(Symbol(LivyClientArgumentKeys.JARS), "").asInstanceOf[String].split(",").toList,
        inputOptions.getOrElse(Symbol(LivyClientArgumentKeys.CONF), "").asInstanceOf[Map[String, String]]
      )

      val state = LivyInteractiveJobAction.start(startSessionUrl, sessionRequest, userCredentials(inputOptions), testMode)
      if (state.isDefined)
        {
          monitorSession(inputOptions)
        }
    }
  }

  /**
    * Stops the current session passed
    * @param inputOptions - list of input options
    */
  def stopSession(inputOptions: LivyClientArgumentParser.ArgumentMap): Unit = {
    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))

    val sessionName = inputOptions(Symbol(LivyClientArgumentKeys.ApplicationName))
    val startSessionUrl = "http://%s/sessions/%s".format(inputOptions(Symbol(LivyClientArgumentKeys.Cluster)), sessionName)

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.StopSession))) {
      LivyInteractiveJobAction.stop(startSessionUrl, userCredentials(inputOptions), testMode)
    }
  }

  /**
    * Executes the given statement through the LIVY session
    * @param inputOptions - list of input options to execute the given statement
    * @return - It returns the state whether idle/available/dead
    */
  def executeStatement(inputOptions: LivyClientArgumentParser.ArgumentMap): Option[State] = {
    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))
    val code = inputOptions(Symbol(LivyClientArgumentKeys.Code)).asInstanceOf[String]
    val sessionName = inputOptions(Symbol(LivyClientArgumentKeys.ApplicationName)).asInstanceOf[String]
    val url = "http://%s/sessions/%s/statements".format(inputOptions(Symbol(LivyClientArgumentKeys.Cluster)), sessionName)
    val statement = LivyInteractiveJobAction.executeStatement(url, Code(code), userCredentials(inputOptions), testMode)
    statement match {
      case None =>
      case Some(state) =>
        monitorStatement(inputOptions, state.id)
    }
    statement
  }

  /**
    * Prints the statment logs from a given livy session
    * @param inputOptions - set of input options
    */
  def getStatements(inputOptions: LivyClientArgumentParser.ArgumentMap): Unit = {
    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))
    val sessionName = inputOptions(Symbol(LivyClientArgumentKeys.ApplicationName)).asInstanceOf[String]
    val url = "http://%s/sessions/%s/statements".format(inputOptions(Symbol(LivyClientArgumentKeys.Cluster)), sessionName)
    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.Statements))) {
      LivyInteractiveJobAction.getStatements(url, inputOptions.get(Symbol(LivyClientArgumentKeys.StatementId)).asInstanceOf[Option[Int]], userCredentials(inputOptions), testMode)
    }
  }

  /**
    * Returns the response string of the log when livy executes a statement
    * @param inputOptions - list of input options
    * @param statementId - statement ID for which the log need to be pulled from Livy session
    * @return - response string from livy statement execution log
    */
  def getStatementsLog(inputOptions: LivyClientArgumentParser.ArgumentMap, statementId: Long): String = {
    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))
    val sessionName = inputOptions(Symbol(LivyClientArgumentKeys.ApplicationName)).asInstanceOf[String]
    val url = "http://%s/sessions/%s/statements".format(inputOptions(Symbol(LivyClientArgumentKeys.Cluster)), sessionName)
    val res: String = LivyInteractiveJobAction.getStatementLog(s"$url/$statementId", userCredentials(inputOptions), testMode)
    res
  }

  /**
    * It keeps polling the livy server to ensure the created session is available
    * @param inputOptions - list of input options
    * @param statementId - statement ID for which the log need to be pulled from Livy session
    */
  def monitorStatement(inputOptions: LivyClientArgumentParser.ArgumentMap, statementId: Long): Unit = {
    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))
    val sessionName = inputOptions(Symbol(LivyClientArgumentKeys.ApplicationName)).asInstanceOf[String]
    val url = "http://%s/sessions/%s/statements".format(inputOptions(Symbol(LivyClientArgumentKeys.Cluster)), sessionName)

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.Wait)) && inputOptions.contains(Symbol(LivyClientArgumentKeys.Statements))) {
      var res: (Option[State], String) = (Some(new State(-1, "UNDEFINED")), "")
      do {
        res = LivyInteractiveJobAction.getStatement(s"$url/$statementId", userCredentials(inputOptions), testMode)
        Thread.sleep(1000)
      } while (!res._1.get.state.equalsIgnoreCase("available") && !res._1.get.state.equalsIgnoreCase("error"))
      JSONUtils.printToJson(res._2)
    }
  }

  /**
    * This monitors the session to ensure it is available for sending code/sqls
    * @param inputOptions - list of input options
    */
  def monitorSession(inputOptions: LivyClientArgumentParser.ArgumentMap): Unit = {
    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))
    val sessionName = inputOptions(Symbol(LivyClientArgumentKeys.ApplicationName)).asInstanceOf[String]
    val url = "http://%s/sessions/%s".format(inputOptions(Symbol(LivyClientArgumentKeys.Cluster)), sessionName)

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.Wait)) && inputOptions.contains(Symbol(LivyClientArgumentKeys.StartSession))) {
      var state: Option[State] = None
      do {
        state = LivyInteractiveJobAction.getSession(s"$url", userCredentials(inputOptions), testMode)
        if (!state.isDefined)
          {
            return
          }
        Thread.sleep(1000)
      } while (!state.get.state.equalsIgnoreCase("idle") && !state.get.state.equalsIgnoreCase("dead"))
    }
  }
}
