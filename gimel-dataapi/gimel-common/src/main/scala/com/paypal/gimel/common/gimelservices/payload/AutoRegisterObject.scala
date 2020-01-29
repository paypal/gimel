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

/*
 * A AutoRegisterObject is an entity in Metastore which would let the poller know what are the datasets
 * which needs to be Auto-registered.
 * Http End point which gives you this payload is http://localhost:8080/objectschema/schemas
 * Sample Response -> [{"objectId": 8, "objectName": "tableName", "containerName": "default","storageSystemId": 56,
 * "clusterId": 5,"query": "CREATE EXTERNAL TABLE "}]
 */
package com.paypal.gimel.common.gimelservices.payload

case class AutoRegisterObject(
                               objectId: Int = 0
                               , objectName: String = ""
                               , containerName: String = ""
                               , storageSystemId: Int = -99
                               , query: String = ""
                               , clusterIds: Seq[Int] = Seq.empty[Int]
                               , objectSchema: Seq[ObjectSchema] = Seq.empty[ObjectSchema]
                               , objectAttributes: Seq[ObjectAttributeValue] = Seq.empty[ObjectAttributeValue]
                               , isActiveYN: String = ""
                               , createdUserOnStore: String
                               , createdTimestampOnStore: String
                               , isSelfDiscovered: String
                             )
