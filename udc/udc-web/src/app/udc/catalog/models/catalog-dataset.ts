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

import {TeradataPolicy} from './catalog-teradata-policy';
import {DerivedPolicy} from './catalog-derived-policy';

export class Dataset {
  public createdUser: string;
  public storageDataSetName: string;
  public storageDataSetDescription: string;
  public storageDataSetId: number;
  public storageSystemId: number;
  public clusters: Array<number>;
  public storageContainerName: string;
  public objectName: string;
  public userId: number;
  public storageClusterId: number;
  public isAutoRegistered: string;
  public storageDataSetAliasName: string;
  public storageDatabaseName: string;
  public storageClusterName: string;
  public storageSystemName: string;
  public storageDatasetStatus: string;
  public createdTimestamp: string;
  public updatedUser: string;
  public attributesPresent: boolean;
  public updatedTimestamp: string;
  public objectSchemaMapId: number;
  public createdUserOnStore: string;
  public createdTimestampOnStore: string;
  public isReadCompatible: string;
  public zoneName: string;
  public isAccessControlled: string;
  public teradataPolicies = new Array<TeradataPolicy>();
  public derivedPolicies = new Array<DerivedPolicy>();
  public isActiveYN: string;
  public entityName: string;


  constructor(storageDataSetName: string, storageDataSetId: number, storageSystemName: string, isAttributesPresent: boolean, objectSchemaMapId: number, storageDataSetAliasName: string, storageDataSetDescription: string, createdUser: string, updatedUser: string, zoneName: string, isAccessControlled: string, teradataPolicies: Array<TeradataPolicy>, derivedPolicies: Array<DerivedPolicy>, isReadCompatible: string, isActiveYN: string, entityName: string) {
    this.storageDataSetName = storageDataSetName;
    this.storageDataSetId = storageDataSetId;
    this.storageSystemName = storageSystemName;
    this.attributesPresent = isAttributesPresent;
    this.objectSchemaMapId = objectSchemaMapId;
    this.storageDataSetAliasName = storageDataSetAliasName;
    this.storageDataSetDescription = storageDataSetDescription;
    this.createdUser = createdUser;
    this.updatedUser = updatedUser;
    this.zoneName = zoneName;
    this.isAccessControlled = isAccessControlled;
    this.teradataPolicies = teradataPolicies;
    this.derivedPolicies = derivedPolicies;
    this.isReadCompatible = isReadCompatible;
    this.isActiveYN = isActiveYN;
    this.entityName = entityName;
  }
}
