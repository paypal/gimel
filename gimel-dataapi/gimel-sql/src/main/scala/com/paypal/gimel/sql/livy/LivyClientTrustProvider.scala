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

import org.apache.http.auth.AuthSchemeProvider
import org.apache.http.client.config.AuthSchemes
import org.apache.http.config.{Lookup, RegistryBuilder}
import org.apache.http.impl.auth.SPNegoSchemeFactory
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}

object LivyClientTrustProvider {
    /**
      * gets the http client object
      * @param testMode - whether test or real mode
      * @return - it returns the http client object
      */
    def getHttpClient(testMode: Boolean): CloseableHttpClient = {
      // Trusts any certificate for use in test mode only
      if (testMode) getOpenHttpClient()
      else {
        val skipPortAtKerberosDatabaseLookup = true
        val authSchemeRegistry: Lookup[AuthSchemeProvider] = RegistryBuilder.create[AuthSchemeProvider]()
          .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(skipPortAtKerberosDatabaseLookup)).build()
        HttpClientBuilder.create().setDefaultAuthSchemeRegistry(authSchemeRegistry).build
      }
    }

    /**
      * gets the open heppt client
      * @return - open http client object
      */
    private def getOpenHttpClient(): CloseableHttpClient = {
      val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
      httpClient
    }
  }
