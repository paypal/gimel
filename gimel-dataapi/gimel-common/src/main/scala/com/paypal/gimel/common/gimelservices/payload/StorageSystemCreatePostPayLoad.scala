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

package com.paypal.gimel.common.gimelservices.payload

case class StorageSystemCreatePostPayLoad(
                                           storageTypeId: Int = -99
                                           , storageSystemName: String = ""
                                           , runningClusterId: Int = -99
                                           , assignedClusterId: Int = -99
                                           , zoneId: Int = -99
                                           , entityId: Int = -99
                                           , storageSystemDescription: String = ""
                                           , createdUser: String = "gimel_user"
                                           , adminUserId: Int = -99
                                           , systemAttributeValues: Seq[StorageSystemAttributeValue] = Seq.empty[StorageSystemAttributeValue]
                                           , containerNames: String = "All"
                                         )
