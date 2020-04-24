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

import org.apache.http.auth.Credentials
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.json4s.jackson.Serialization.write

class LivyInteractiveMessages()

case class SessionRequest(
                           var proxyUser: String,
                           var kind: String,
                           var name: String,
                           var jars: List[String],
                           var conf: Map[String, String])

case class Code(code: String)

case class State(id: Long, state: String)

object LivyInteractiveJobAction extends Actions {

  /**
    * Prints the list of sessions available
    * @param listUrl - URL of the livy end point
    * @param userDetails - user credentials
    * @param testMode - test mode or real mode call
    */
  def list(listUrl: String, userDetails: Credentials, testMode: Boolean = false): Unit = {

    val httpClient: CloseableHttpClient = LivyClientTrustProvider.getHttpClient(testMode)
    val httpResponse = httpClient.execute(new HttpGet(listUrl), getContext(userDetails))
    printResponse(httpResponse)
    httpClient.close()

  }

  /**
    * Gets the state of the current livy session
    * @param listUrl - URL of the livy end point
    * @param userDetails - user credentials
    * @param testMode - test mode or real mode call
    * @return - Returns the state whether the session is idle/available/dead
    */
  def getSession(listUrl: String, userDetails: Credentials, testMode: Boolean = false): Option[State] = {
    val httpClient: CloseableHttpClient = LivyClientTrustProvider.getHttpClient(testMode)
    val httpResponse = httpClient.execute(new HttpGet(listUrl), getContext(userDetails))

    val response = printResponse(httpResponse)
    httpClient.close()
    JSONUtils.convertToJson[State](response)
  }

  /**
    * Starts the livy session
    * @param url - URL of the livy end point
    * @param request - Session request object
    * @param userDetails - user credentials
    * @param testMode - test mode or real mode call
    * @return -  Returns the state whether the session is idle/available/dead
    */
  def start(url: String, request: SessionRequest, userDetails: Credentials, testMode: Boolean = false): Option[State] = {

    val postRequest = new HttpPost(url)
    postRequest.addHeader("Content-Type", "application/json")
    postRequest.setEntity(new StringEntity(write(request)))

    val httpClient: CloseableHttpClient = LivyClientTrustProvider.getHttpClient(testMode)

    val httpResponse = httpClient.execute(postRequest, getContext(userDetails))

    val response = printResponse(httpResponse)
    httpClient.close()
    JSONUtils.convertToJson[State](response)
  }

  /**
    * stops the current session
    * @param url - URL of the livy end point
    * @param userDetails - user credentials
    * @param testMode - test mode or real mode call
    */
  def stop(url: String, userDetails: Credentials, testMode: Boolean): Unit = {
    val deleteRequest = new HttpDelete(url)
    val httpClient = LivyClientTrustProvider.getHttpClient(testMode)

    val httpResponse = httpClient.execute(deleteRequest, getContext(userDetails))

    printResponse(httpResponse)
    httpClient.close()

  }

  /**
    * Executes the given code/sql in the existing livy session
    * @param url - URL of the livy end point
    * @param code - sql/code
    * @param userDetails - user credentials
    * @param testMode - test mode or real mode call
    * @return - Returns the state whether the session is idle/available/dead
    */
  def executeStatement(url: String, code: Code, userDetails: Credentials, testMode: Boolean): Option[State] = {
    val postRequest = new HttpPost(url)
    val httpClient = LivyClientTrustProvider.getHttpClient(testMode)
    postRequest.addHeader("Content-Type", "application/json")
    postRequest.setEntity(new StringEntity(write(code)))

    val httpResponse = httpClient.execute(postRequest, getContext(userDetails))

    val statement: Option[State] = getResponse[State](httpResponse)
    httpClient.close()
    if (statement.isDefined) {
      JSONUtils.printToJson(statement.get)
    }
    statement
  }

  /**
    * Prints all the statements currently executed for the session
    * @param url - URL of the livy end point
    * @param statementId - statement ID
    * @param userDetails - user credentials
    * @param testMode - test mode or real mode call
    */
  def getStatements(url: String, statementId: Option[Int], userDetails: Credentials, testMode: Boolean): Unit = {
    var updatedUrl = url
    statementId match {
      case Some(id) =>
        updatedUrl = s"$url/$id"
      case None =>
    }
    val getRequest = new HttpGet(updatedUrl)
    val httpClient = LivyClientTrustProvider.getHttpClient(testMode)
    val httpResponse = httpClient.execute(getRequest, getContext(userDetails))
    printResponse(httpResponse)
    httpClient.close()
  }

  /**
    * gets the state and response of the supplied statement of the given livy session
    * @param url - - URL of the livy end point
    * @param userDetails - user credentials
    * @param testMode - test mode or real mode call
    * @return - state and http reponse string
    */
  def getStatement(url: String, userDetails: Credentials, testMode: Boolean): (Option[State], String) = {
    val getRequest = new HttpGet(url)
    val httpClient = LivyClientTrustProvider.getHttpClient(testMode)
    val response = httpClient.execute(getRequest, getContext(userDetails))
    var responseContent = "No response"
    val statusCode = response.getStatusLine.getStatusCode
    var state: Option[State] = None
    if (statusCode == 200 || statusCode == 201) {
      if (response.getEntity != null) {
        val inputStream = response.getEntity.getContent
        responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        state = JSONUtils.convertToJson[State](responseContent)
        if (state.isDefined)
          {
            JSONUtils.printToJson(state.get)
          }
      }
    }
    httpClient.close()
    (state, responseContent)
  }

  /**
    * Gets the log of the statements execution result
    * @param url - - URL of the livy end point
    * @param userDetails - user credentials
    * @param testMode - test mode or real mode call
    * @return - response from the livy statement log call
    */
  def getStatementLog(url: String, userDetails: Credentials, testMode: Boolean): String = {
    val getRequest = new HttpGet(url)
    val httpClient = LivyClientTrustProvider.getHttpClient(testMode)
    val response = httpClient.execute(getRequest, getContext(userDetails))
    var responseContent = "No response"
    val statusCode = response.getStatusLine.getStatusCode
    var state: Option[State] = None
    if (statusCode == 200 || statusCode == 201) {
      if (response.getEntity != null) {
        val inputStream = response.getEntity.getContent
        responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      }
    }
    httpClient.close()
    responseContent
  }
}



