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

package com.paypal.gimel.common.storageadmin

import java.util.Calendar

import com.aerospike.client.{Bin, Host, Key, Record}
import com.aerospike.client.async.{AsyncClient, AsyncClientPolicy}
import com.aerospike.client.policy.{BatchPolicy, InfoPolicy, Policy, ScanPolicy, WritePolicy}

object AerospikeAdminClient {

  /**
    * Creates Aerospike client connection
    *
    * @param aerospikeClientIP   Aerospike client host
    * @param aerospikeClientPort Aerospike client port
    */
  def createClientConnection(aerospikeClientIP: String, aerospikeClientPort: Int): AsyncClient = {
    val client = new AsyncClient(aerospikeClientIP, aerospikeClientPort)
    client
  }

  /**
    * Performs lookup on aerospike
    *
    * @param client Aerospike client
    * @param policy Aerospike Read Policy
    * @param key    key to lookup
    */
  def get(client: AsyncClient, policy: Policy, key: Key): Record = {
    val record = client.get(policy, key)
    record
  }

  /**
    * Performs batch lookup on aerospike
    *
    * @param client      Aerospike client
    * @param batchPolicy Aerospike Batch Policy
    * @param keys        array of keys to perform batch get
    */
  def batchGet(client: AsyncClient, batchPolicy: BatchPolicy, keys: Array[Key]): Array[Record] = {
    val records = client.get(batchPolicy, keys)
    records
  }

  /**
    * Performs lookup on aerospike
    *
    * @param aerospikeClientIP   Aerospike client host
    * @param aerospikeClientPort Aerospike client port
    * @param policy              Aerospike Read Policy
    * @param key                 key to lookup
    *
    */
  def get(aerospikeClientIP: String, aerospikeClientPort: Int, policy: Policy, key: Key): Record = {
    val client = createClientConnection(aerospikeClientIP, aerospikeClientPort)
    val record = client.get(policy, key)
    client.close()
    record
  }

  /**
    * Performs batch lookup on aerospike
    *
    * @param aerospikeClientIP   Aerospike client host
    * @param aerospikeClientPort Aerospike client port
    * @param batchPolicy         Aerospike Batch Policy
    * @param keys                array of keys to perform batch get
    */
  def batchGet(aerospikeClientIP: String, aerospikeClientPort: Int, batchPolicy: BatchPolicy, keys: Array[Key]): Array[Record] = {
    val client = createClientConnection(aerospikeClientIP, aerospikeClientPort)
    val records = client.get(batchPolicy, keys)
    client.close()
    records
  }

  /**
    * writes/updates record in aerospike
    *
    * @param aerospikeClientIP   Aerospike client host
    * @param aerospikeClientPort Aerospike client port
    * @param writePolicy         Aerospike write Policy
    * @param key                 key to be written
    */
  def put(aerospikeClientIP: String, aerospikeClientPort: Int, writePolicy: WritePolicy, key: Key, bins: Array[Bin]): Unit = {
    val client = createClientConnection(aerospikeClientIP, aerospikeClientPort)
    client.put(writePolicy, key, bins: _*)
    client.close()
  }

  /**
    * writes/updates record in aerospike
    *
    * @param client      Aerospike client
    * @param writePolicy Aerospike write Policy
    * @param key         key to be written
    */
  def put(client: AsyncClient, writePolicy: WritePolicy, key: Key, bins: Array[Bin]): Unit = {
    client.put(writePolicy, key, bins: _*)
  }

  /**
    * deletes set from aerospike
    * This is commented for now as Aerospike client used in Aerospark uses 3.1.5 version which doesnt support truncate
    *
    * @param aerospikeClientIP   Aerospike client host
    * @param aerospikeClientPort Aerospike client port
    * @param infoPolicy          Aerospike Info Policy
    * @param namespace           Aerospike namespace
    * @param setName             name of the set for dedup records
    */
  def deleteSet(aerospikeClientIP: String, aerospikeClientPort: Int, infoPolicy: InfoPolicy, namespace: String, setName: String): Unit = {
    //    val client = createClientConnection(aerospikeClientIP, aerospikeClientPort)
    //    client.truncate(infoPolicy, namespace, setName, Calendar.getInstance())
    //    client.close()
  }
}
