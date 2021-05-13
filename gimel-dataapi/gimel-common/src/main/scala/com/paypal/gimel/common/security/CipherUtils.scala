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

package com.paypal.gimel.common.security

import java.security.{KeyFactory, PrivateKey}
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import javax.crypto.Cipher

import com.paypal.gimel.common.conf.GimelConstants

object CipherUtils {

  /**
   * Check if Big Query Table name is supplied. If not supplied : fail.
   * @param key The b64 encoded privateKey
   * @param cipherText The b64 encoded cipherText
   * @param algorithm Option[Algorithm]. Example Some("RSA"). On None, default to "RSA"
   * @return PlainText Value
   */

  def decrypt(key: String, cipherText: String, algorithm: Option[String] = None): String = {
    val resolvedAlgorithm = algorithm match {
      case Some(x) =>
        if (x != GimelConstants.RSA) throw new Exception(s"Unsupported Type [${x}]")
        else x
      case _ => GimelConstants.RSA
    }
    val pK: Array[Byte] = Base64.getDecoder().decode(key)
    val privateKey: PrivateKey = KeyFactory.getInstance(resolvedAlgorithm).generatePrivate(new PKCS8EncodedKeySpec(pK))
    val cipher: Cipher = Cipher.getInstance(resolvedAlgorithm)
    cipher.init(Cipher.DECRYPT_MODE, privateKey)

    val decoded: Array[Byte] = Base64.getDecoder().decode(cipherText)
    val plainText: Array[Byte] = cipher.doFinal(decoded)
    plainText.map(_.toChar).mkString
  }

}
