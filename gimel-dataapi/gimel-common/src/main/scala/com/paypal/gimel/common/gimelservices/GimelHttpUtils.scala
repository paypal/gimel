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

package com.paypal.gimel.common.gimelservices

import java.io.IOException
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}

import org.apache.http.HttpResponse
import org.apache.http.client.{ClientProtocolException, ResponseHandler}
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.config.RegistryBuilder
import org.apache.http.ssl.SSLContexts

import com.paypal.gimel.common.conf.GimelTypeRefConstants
import com.paypal.gimel.common.gimelservices.payload.Zone
import com.paypal.gimel.common.utilities.GenericUtils

object GimelHttpUtils {

  import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
  import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
  import org.apache.http.impl.conn.PoolingHttpClientConnectionManager

  private val sslContext: SSLContext = SSLContexts.custom().build()

  import org.apache.http.conn.ssl.SSLConnectionSocketFactory

  sslContext.init(null, Array[TrustManager](new X509TrustManager() {
    def getAcceptedIssuers: Array[X509Certificate] = {
//      getAcceptedIssuers =============
      null
    }
    def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = {
//      "checkClientTrusted ============="
    }
    def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = {
//      "checkServerTrusted ============="
    }
  }), new SecureRandom())
  val sslConnectionSocketFactory =
    new SSLConnectionSocketFactory(sslContext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)

  val connectionManager = new PoolingHttpClientConnectionManager(
    RegistryBuilder.create[ConnectionSocketFactory].register("http",
      PlainConnectionSocketFactory.getSocketFactory).register("https", sslConnectionSocketFactory).build)

  private val client: CloseableHttpClient = HttpClients.custom.setConnectionManager(connectionManager).build
  private val httpClient = HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build()

  def executeGet[T](httpUriRequest: HttpUriRequest,
                    responseHandler: ResponseHandler[T]): T = {
    client.execute(httpUriRequest, responseHandler)
  }

  // Utilities for building the URI request

  // Utilities for handling response handler
  val SEQ_ZONE_RESPONSE_HANDLER: ResponseHandler[Seq[Zone]] =
    new ResponseHandler[Seq[Zone]]() {
      @throws[ClientProtocolException]
      @throws[IOException]
      def handleResponse(response: HttpResponse): Seq[Zone] = {
        val status = response.getStatusLine.getStatusCode
        if (status >= 200 && status < 300) {
          val entity = response.getEntity
          if (entity != null) {
            GenericUtils.fromString(
              entity.getContent,
              GimelTypeRefConstants.SEQ_ZONE_TYPE_REFERENCE
            )
          } else {
            null
          }
        } else {
          throw new ClientProtocolException(
            s"Unexpected response status -> Code : $status | Reason : ${response.getStatusLine.getReasonPhrase}"
          )
        }
      }
    }

  val GENERIC_RESPONSE_HANDLER: ResponseHandler[Map[String, Any]] =
    new ResponseHandler[Map[String, Any]]() {
      @throws[ClientProtocolException]
      @throws[IOException]
      def handleResponse(response: HttpResponse): Map[String, Any] = {
        val status = response.getStatusLine.getStatusCode
        if (status >= 200 && status < 300) {
          val entity = response.getEntity
          if (entity != null) {
            GenericUtils.fromString(
              entity.getContent,
              GimelTypeRefConstants.MAP_ANY_TYPE_REFERENCE
            )
          } else {
            null
          }
        } else {
          throw new ClientProtocolException(
            s"Unexpected response status -> Code : $status | Reason : ${response.getStatusLine.getReasonPhrase}"
          )
        }
      }
    }
}
