/*
 * Copyright 2019 PayPal Inc.
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

import { Schema } from './catalog-schema';
import { ObjectAttributeValue } from './catalog-object-attribute-value';

export class ElasticSearchDataset {
  public _id: string;
  public storageDataSetId: number;
  public storageDataSetName: string;
  public storageDataSetAliasName: string;
  public storageDatabaseName: string;
  public storageDataSetDescription: string;
  public isActiveYN: string;
  public userId: number;
  public createdUser: string;
  public createdTimestamp: string;
  public updatedUser: string;
  public updatedTimestamp: string;
  public isAutoRegistered: string;
  public objectSchemaMapId: number;
  public storageSystemId: number;
  public zoneId: number;
  public isReadCompatible: string;
  public containerName: string;
  public objectName: string;
  public isSelfDiscovered: string;
  public isRegistered: string;
  public createdUserOnStore: string;
  public createdTimestampOnStore: string;
  public objectSchema = new Array<Schema>();
  public objectAttributeValues = new Array<ObjectAttributeValue>();

  constructor(storageDataSetId: number, storageDataSetName: string, storageDataSetAliasName: string, storageDatabaseName: string, storageDataSetDescription: string, isActiveYN: string, userId: number, createdUser: string, createdTimestamp: string, updatedUser: string, updatedTimestamp: string, isAutoRegistered: string, objectSchemaMapId: number, storageSystemId: number, zoneId: number, isReadCompatible: string, containerName: string, objectName: string, isSelfDiscovered: string, isRegistered: string, createdUserOnStore: string, createdTimestampOnStore: string, objectSchema: Schema[], objectAttributeValues: ObjectAttributeValue[]) {
    this.storageDataSetId = storageDataSetId;
    this.storageDataSetName = storageDataSetName;
    this.storageDataSetAliasName = storageDataSetAliasName;
    this.storageDatabaseName = storageDatabaseName;
    this.storageDataSetDescription = storageDataSetDescription;
    this.isActiveYN = isActiveYN;
    this.userId = userId;
    this.createdUser = createdUser;
    this.createdTimestamp = createdTimestamp;
    this.updatedUser = updatedUser;
    this.updatedTimestamp = updatedTimestamp;
    this.isAutoRegistered = isAutoRegistered;
    this.objectSchemaMapId = objectSchemaMapId;
    this.storageSystemId = storageSystemId;
    this.zoneId = zoneId;
    this.isReadCompatible = isReadCompatible;
    this.containerName = containerName;
    this.objectName = objectName;
    this.isSelfDiscovered = isSelfDiscovered;
    this.isRegistered = isRegistered;
    this.createdUserOnStore = createdUserOnStore;
    this.createdTimestampOnStore = createdTimestampOnStore;
    this.objectSchema = objectSchema;
    this.objectAttributeValues = objectAttributeValues;
  }
}
