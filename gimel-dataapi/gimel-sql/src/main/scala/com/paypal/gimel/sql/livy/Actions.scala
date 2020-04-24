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

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.auth.{AuthScope, Credentials}
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.impl.client.BasicCredentialsProvider
import org.json4s.ShortTypeHints
import org.json4s.jackson.Serialization

import com.paypal.gimel.logger.Logger


abstract class Actions {
  val mapper: ObjectMapper = new ObjectMapper()
  val logger: Logger = Logger(this.getClass.getName)

  implicit val formats = Serialization.formats(
    ShortTypeHints(
      List()
    )
  )

  /**
    * Converts the CloseableHttpResponse to Json and prints
    *
    * @param response - returns the response from the httpresponse
    */
  def printResponse(response: CloseableHttpResponse): String = {
    val statusCode = response.getStatusLine.getStatusCode
    var responseContent = "No Response"
    if (statusCode == 200 || statusCode == 201) {
      if (response.getEntity != null) {
        val inputStream = response.getEntity.getContent
        responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        inputStream.close()
        try {
          val obj = mapper.readValue(responseContent, classOf[Object])
          logger.info(mapper.writerWithDefaultPrettyPrinter.writeValueAsString(obj))
        } catch {
          case a: JsonParseException =>
            logger.info(responseContent)
        }
      } else {
        logger.info(responseContent)
      }
    } else {
      val inputStream = response.getEntity.getContent
      responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close()
      logger.info("response content" +  responseContent)
      logger.info(s"http response status code $statusCode")
    }
    responseContent
  }

  /**
    * Parses the Http response JSON
    * @param response - Response from the HTTP call
    * @tparam T - the scala class to be casted to
    * @return - the scala object
    */
  def getResponse[T: Manifest](response: CloseableHttpResponse): Option[T] = {
    val statusCode = response.getStatusLine.getStatusCode
    var res: Option[T] = None
    if (statusCode == 200 || statusCode == 201) {
      if (response.getEntity != null) {
        val inputStream = response.getEntity.getContent
        val responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        inputStream.close()
        try {
          res = JSONUtils.convertToJson[T](responseContent)
        } catch {
          case a: JsonParseException =>
            logger.info(responseContent)
        }
      } else {
        logger.info("No Response")
      }
    } else {
      logger.info(s"http response status code $statusCode")
    }
    res
  }

  /**
    * Prints the scala object as JSON string
    * @param data -  the state object
    */
  def printToJson(data: State): Unit = {
    try {
      logger.info(mapper.writerWithDefaultPrettyPrinter.writeValueAsString(data))
    } catch {
      case exec: JsonParseException =>
        logger.info(data)
    }
  }

  /**
    * Gets the credentials and provide context
    * @param userDetails - Incoming user name and credentials
    * @return - The http clint context object
    */
  def getContext(userDetails: Credentials): HttpClientContext = {
    val context = HttpClientContext.create()
    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(new AuthScope(null, -1, null), userDetails)
    context.setCredentialsProvider(credentialsProvider)
    context
  }
}
