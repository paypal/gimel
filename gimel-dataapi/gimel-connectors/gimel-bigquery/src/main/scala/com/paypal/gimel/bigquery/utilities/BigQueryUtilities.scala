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

package com.paypal.gimel.bigquery.utilities

import com.google.api.client.json.GenericJson
import java.util.Base64
import org.apache.commons.lang3.CharEncoding.UTF_8

import com.paypal.gimel.bigquery.conf.{BigQueryConfigs, BigQueryConstants}
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.logger.Logger

object BigQueryUtilities {

  val logger = Logger()

  /**
   * Check if Big Query Table name is supplied. If not supplied : fail.
   * @param bigQueryTable The Big Query Table Name in the form of "gimel.bigquery.table"
   */
  def failIfTableNotSpecified(bigQueryTable: Option[String]): Unit = {
    bigQueryTable match {
      case Some(tbl) => logger.info(s"Found Table ${bigQueryTable.get}")
      case _ => throw new IllegalArgumentException(
        s"""Big Query Connector Requires Table Name. Please pass the option [${BigQueryConfigs.bigQueryTable}] in the write API.""")
    }
  }

  /**
   * Parses the Save Mode for the write functionality
   * @param saveMode Spark's Save mode for Sink (such as Append, Overwrite ...)
   * @param bigQueryTable The Big Query Table Name in the form of "gimel.bigquery.table"
   */
  def parseSaveMode(saveMode: String, bigQueryTable: String): Unit = {
    saveMode match {
      case BigQueryConstants.saveModeAppend =>
        logger.info(s"Appending to Big Query table [${bigQueryTable}]")
      case BigQueryConstants.saveModeErrorIfExists =>
        logger.info(s"Asked to Err if exists Big Query table [${bigQueryTable}]")
      case BigQueryConstants.saveModeOverwrite =>
        logger.info(s"Overwriting Big Query table [${bigQueryTable}]")
      case BigQueryConstants.saveModeIgnore =>
        logger.info(s"Asked to Ignore if exists Big Query table [${bigQueryTable}]")
      case _ => throw new IllegalArgumentException(
        s"""Illegal [saveMode]:[${saveMode}]
           |${BigQueryConstants.bigQueryDocUrl}
           |""".stripMargin)
    }
  }

  def withCred(options: Map[String, String]): Map[String, String] = {

    val json = new GenericJson()
    // @todo Move "gimel.bigquery.auth.provider.class" to GimelConf
    val authProviderClass = options.getOrElse("gimel.bigquery.auth.provider.class", GimelConstants.DEFAULT_AUTH_PROVIDER_CLASS)
    var authLoader: com.paypal.gimel.common.security.AuthProvider = null
    if (options.getOrElse("gimel.load.auth.provider","FALSE").toUpperCase().equals("TRUE")) {
      logger.info("LOADING AUTH PROVIDER")
      authLoader = Class.forName(authProviderClass).newInstance.asInstanceOf[com.paypal.gimel.common.security.AuthProvider]
    } else logger.info("NOT LOADING AUTH PROVIDER")

    // @todo Remove Hardcoding of token
    val appToken = ""

    val keyMakerOptionsClientId = Map("gimel.keymaker.appkey" -> options.getOrElse("gimel.keymaker.appkey", ""),
      "gimel.keymaker.apptoken" -> options.getOrElse("gimel.keymaker.apptoken", ""),
      "gimel.keymaker.url" -> options.getOrElse("gimel.keymaker.url", "")
    )
    val keyMakerOptionsClientSecret = keyMakerOptionsClientId ++ Map("gimel.keymaker.appkey" -> options.getOrElse("gimel.keymaker.appkey", ""))
    val keyMakerOptionsClientPrivateKey = keyMakerOptionsClientId ++ Map("gimel.keymaker.appkey" -> options.getOrElse("gimel.keymaker.appkey", ""))

    // val clntId = authLoader.getCredentials(keyMakerOptionsClientId)
    // val clntS = authLoader.getCredentials(keyMakerOptionsClientSecret)
    // val privateKey = authLoader.getCredentials(keyMakerOptionsClientPrivateKey)

    // @todo replace with keymaker call
    val clntId: String = "878933003531-dnr2loiobiddo04fr37nh73h0pv9gip2.apps.googleusercontent.com"
    val clntS: String = "CQCLgC0aZHywfsFpOMhE8is3"
    val privateKey = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCQb4C0wLOIAWo9eC779CS/ViZRvias2H/7+aBkAieCKhayJJyVzRed7BkYTw59Mkd6hFtFuiTtDDZRv6v6UuHFr4w1go+X3WTTBtAtIi3gSW7LP1PLb4r/ryboiby0JrcOPfgsuEOu/b1ccxri+nHB1o7wZEuwCrcS4QYMFjPFx4ru1BCua0zUBSVn/kw0bStVYzzKdSRJ3iKNtbAcQcOuInci/ejVnXmG4YiXiayyo2XWAYgxzPyl+4Fpu/CiegxK85jGtQDxFdLuAaFaB7AtswbQs3yKPHtclXlnhBb3BGubVrvbBM9DqyKNTLgr7W9aIJTI0jzPR9bqpaNikSANAgMBAAECggEAWK39Sf1evUyRHDikMFHQFiIg2ON0/37V5bF4hq7D7ylEUhAki90mePGy3rf7X2b1eAW3vHHzpg9vcnXKc6FbuXJ1FY2Z+FYYR3DRdESeAi1T9zpkim1r3Fx/+RpIYBu9HONzcpGYDOMB1rdddKrsGWVQ0cvipK2PhenfOqCPHQjZ0QtCz/IrrZQaCvlVq9O7qzVyvgXNzQaiPLvTli4ti7S2ZQEDPYl8UZ4BiSw+Dbav+shPZyhLSDT32itD6XAUTGLP2BCm3H1NGCWGsHTAekaVWGzwj/mcRyGgX7gS02qpKA3Te69vAoh5YvLSCsEplEuDnc7khtQRVcWH5iBowQKBgQD1gpYxBjAERpP52+z8EkgENEPAHKMFGZntk4Y/fj4N3lycBfe937NmIeUN6dLSWEaP1lF3ezHwtirnhiLmoa8pLTQNZX535dDL7GZXlo+op6azvjCXRC/gLaHL0vum429eFFhFOsZggP2bI1pgyjxI9xq8LghTJ3to5RdzDhIXRQKBgQCWm1pWvHG4Zo7buIe/IfiE7UbyA1ZXT2Mk9iA4OSAGA3hTf+vJ7zQ73QmxQh58BzEkhhRyL0wnOilJ/6xXYAIpxW9Asf8TtMGTS4sivP7p5RrE2/Xg9EtpY3czD+rlF6G/f1cnlfTyCYRq03fGUI60AvkSTQb/+cPAMjmTCNkuKQKBgQDSM+zELLgP8R3hYBuX908Rym33no01YKYac6UN19jppulD7RggydegKoUjVH/c+RfxL16xHhm0L0Ss1nwrW2PNrZZTogKWRX5wGwfFFnQJwwFIBB82ZHtZRbix+wLb8P75XhH1tE0Fc2uv2KUZGg5jqq6JUCBwke1n8j4RlIqIwQKBgBbL7SCz5YLEA1u+0s1blwKH5/U6DBerLJarqrTX8MD4RX5eHpKyYnWtP4pVN8gOTqH4qZ+fCSfm5dkNmkiff7RS7kQcrT+OXL6u8KCRewRsaWDi6pTiZYfORny0LBoBObqCy+5yBGGejyycVcTu7KrSyGC8yBJ2++pbr9tRu44BAoGAbGj1a5iD1EJ/SYoiDO0TgKvP67D8JctLbOvaeY5JW17PZdGbecOPFtnDH70Djq7NlCA49COinLgXnaVBPvvwxrKDUIweuFK53wFvgdXetAWVgWYnk8d7fuXsj2Tj76ux60yTpUKcTHTPg7BO0VKRdw0AwhoWxMTMZONdqHAalVM="

    // @todo make this generic to work access token
    // @todo token encrypted / decrypted
    val rfrshTknEnc = options.get("gimel.bigquery.refresh.token").get

    // @todo replace with keymaker call
    val rfrshTkn: String = "1//06DLndNxPJQDBCgYIARAAGAYSNwF-L9Ir2SG0DRbeHzUb6hhyyYyu3c0CXAeDRcr13LolG1wIb4SWOouvu7Vgn8XUTJIb7H56Af0"
    // val rfrshTkn = CypherUtils.decrD(privateKey, rfrshTknEnc)

    json.put(GimelConstants.CLIEND_ID, clntId); // Get from keymaker and decode
    json.put(GimelConstants.CLIENT_SECRET, clntS) // Get from keymaker and decode
    json.put(GimelConstants.REFRESH_TOKEN, rfrshTkn)
    json.put(GimelConstants.TYPE, GimelConstants.AUTHORIZED_USER)
    val credKey: String = Base64.getEncoder().encodeToString(json.toString().getBytes(UTF_8))
    options ++ Map("credentials" -> credKey)
  }

}
