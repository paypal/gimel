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

import {Injectable} from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {ApiService} from '../../../core/services';
import {Dataset} from '../models/catalog-dataset';
import {ElasticSearchDataset} from '../models/catalog-dataset-es';
import {ObjectSchemaMap} from '../models/catalog-objectschema';
import {Cluster} from '../../../admin/models/catalog-cluster';
import {User} from '../../../admin/models/catalog-user';
import {Category} from '../../../admin/models/catalog-category';
import {Type} from '../../../admin/models/catalog-type';
import {System} from '../../../admin/models/catalog-system';
import {DatasetWithAttributes} from '../models/catalog-dataset-attributes';
import {PagedData} from '../models/catalog-paged-data';
import {Page} from '../models/catalog-list-page';
import {StorageTypeAttribute} from '../models/catalog-dataset-storagetype-attribute';
import {Zone} from '../../../admin/models/catalog-zone';
import {ObjectAttributeKeyValue} from '../models/catalog-object-attribute-key-value';
import {Entity} from '../../../admin/models/catalog-entity';
import {Notification} from '../../../admin/models/catalog-notification';
import {DatasetTagMapInput} from '../../../admin/models/catalog-dataset-tag-map-input';
import {TeradataDescription} from '../../../admin/models/catalog-teradata-description';
import {TeradataObjectDescription} from '../../../admin/models/catalog-teradata-object-description';
import {DatasetObjectDescription} from '../../../admin/models/catalog-dataset-object-description';
import {DatasetDescription} from '../../../admin/models/catalog-dataset-description';
import {DatasetComment} from '../../../admin/models/catalog-dataset-description-comment';
import {environment} from '../../../../environments/environment';
import {Ownership} from '../../../admin/models/catalog-ownership';
import {Classification} from '../../../admin/models/catalog-classification';

@Injectable()
export class CatalogService {
  private datasetPostAttributesURL: any;
  private actionDetailsURL: string;
  private actionDBDetailsURL: string;
  private actionBackupDetailsURL: string;
  private dataSetsURL: string;
  private dataSetsURLV2: string;
  private objectListURL: string;
  private esDatasetURL: string;
  private dataSetsPendingURL: string;
  private dataSetsDeletedURL: string;
  private dataSetURL: string;
  private dataSetPendingURL: string;
  private dataSetDetailsURL: string;
  private datasetsPrefixUrl: string;
  private objectDetailsURL: string;
  private notificationDetailsURL: string;
  private customAttributeURL: string;
  private customAttributePutURL: string;
  private clustersURL: string;
  private notificationsURL: string;
  private zonesURL: string;
  private usersURL: string;
  private storageCategoriesURL: string;
  private storageTypesURL: string;
  private storageSystemsURL: string;
  private accessControlURL: string;
  private teradataAccessControlURL: string;
  private distinctObjectAttributesURL: string;
  private storageTypeURLForCategory: string;
  private storageTypeURLForCategoryName: string;
  private storageSystemURLForTypeName: string;
  private storageSystemDiscoveryURL: string;
  private storageSystemsURLForType: string;
  private storageSystemURLForZoneName: string;
  private containersURLForSystem: string;
  private containersURLForSystemList: string;
  private storageSystemURL: string;
  private containersURL: string;
  private objectsForContainerAndSystem: string;
  private storageTypeAttributesForType: string;
  private userNameURL: string;
  private datasetPostURL: string;
  private teradataColumnDescriptionPutURL: string;
  private teradataObjectDescriptionPostURL: string;
  private teradataColumnDescriptionPostURL: string;
  private datasetObjectDescriptionPutURL: string;
  private datasetColumnDescriptionPostURL: string;
  private teradataObjectDescriptionPutURL: string;
  private objectsPostURL: string;
  private objectAttributePostURL: string;
  public clustersByNameURL: string;
  public deleteDatasetURL: string;
  private deleteTypeURL: string;
  private deleteSystemURL: string;
  private enableTypeURL: string;
  private enableSystemURL: string;
  public serverWithPort: string;
  private clusterPostURL: string;
  private ownershipPostURL: string;
  private datasetTagPostURL: string;
  private zonePostURL: string;
  private insertUserURL: string;
  private deleteClusterURL: string;
  private deleteZoneURL: string;
  private deleteUserURL: string;
  private deleteCategoryURL: string;
  private enableClusterURL: string;
  private enableZoneURL: string;
  private enableUserURL: string;
  private enableCategoryURL: string;
  private userPostURL: string;
  private categoryPostURL: string;
  private typePostURL: string;
  private typeAttributePostURL: string;
  private systemPostURL: string;
  private schemaByObjectIdURL: string;
  private systemAttributesByNameURL: string;
  private typeAttributesByURL: string;
  private typeAttributesAtSystemLevelByURL: string;
  private dataSetDetails: any;
  private esDataSetDetails: any;
  private objectDetails: any;
  private classificationDetails: any;
  private uniqueDatasetCountURL: string;
  private clustersByIdURL: string;
  private zoneByIdURL: string;
  private teradataDescriptionByDatasetURL: string;
  private teradataColumnDescriptionByDatasetURL: string;
  private datasetColumnDescriptionByDatasetURL: string;
  private datasetDescriptionByDatasetIdURL: string;
  private entityPostURL: string;
  private notificationURL: string;
  private notificationPutURL: string;
  private OwnershipPostURL: string;
  private notificationPostURL: string;
  private notificationBulkUploadURL: string;
  private deleteEntityURL: string;
  private enableEntityURL: string;
  private entityByIdURL: string;
  private entitiesURL: string;
  private classificationURL: string;
  private classificationURLByClassID: string;
  private classificationPostURL: string;
  private classificationBulkPostURL: string;
  private classificationPutURL: string;
  private changeLogByDatasetURL: string;
  private tableDatastoreMapURL: string;
  private pagedObjectData = new PagedData<ObjectSchemaMap>();
  private pagedClassificationDetails = new PagedData<Classification>();
  private pagedData = new PagedData<Dataset>();
  private pagedEsData = new PagedData<ElasticSearchDataset>();
  private timelineListURL: string;

  constructor(private api: ApiService) {
    this.serverWithPort = api.serverWithPort;
    const urlGenerator = (url) => {
      return `${this.serverWithPort}${url}`;
    };
    this.timelineListURL = this.serverWithPort + '/dataSet/timelineList/';
    this.dataSetsURL = urlGenerator('/dataSet/dataSetsBySystem/$typeName$/$systemName$');
    this.dataSetsURLV2 = urlGenerator('/dataSet/dataSets/v2/');
    this.objectListURL = urlGenerator('/objectschema/objects/$systemName$');
    this.esDatasetURL = urlGenerator('/dataSet/dataSetsFromES/');
    this.typeAttributesByURL = urlGenerator('/storageType/storageAttributeKey/$storageTypeId$');
    this.typeAttributesAtSystemLevelByURL = urlGenerator('/storageType/storageAttributeKey/$storageTypeId$/$isSystemLevel$');
    this.dataSetsPendingURL = this.serverWithPort + '/dataSet/dataSetsBySystemPending/$typeName$/$systemName$';
    this.dataSetsDeletedURL = this.serverWithPort + '/dataSet/dataSetsBySystemDeleted/$typeName$/$systemName$';
    this.dataSetURL = this.serverWithPort + '/dataSet/dataSet/$datasetId$';
    this.dataSetPendingURL = this.serverWithPort + '/dataSet/dataSetPending/$datasetId$';
    this.dataSetDetailsURL = this.serverWithPort + '/dataSet/dataSetByName/$datasetName$';
    this.datasetsPrefixUrl = this.serverWithPort + '/dataSet/detailedDataSets/';
    this.objectDetailsURL = this.serverWithPort + '/objectschema/schema/$objectId$';
    this.notificationDetailsURL = this.serverWithPort + '/notification/notification/$notificationId$';
    this.customAttributeURL = this.serverWithPort + '/objectschema/customAttribute/$objectId$';
    this.customAttributePutURL = this.serverWithPort + '/objectschema/customAttribute';
    this.clustersURL = this.serverWithPort + '/cluster/clusters';
    this.notificationsURL = this.serverWithPort + '/notification/notifications';
    this.zonesURL = this.serverWithPort + '/zone/zones';
    this.usersURL = this.serverWithPort + '/user/users';
    this.storageCategoriesURL = this.serverWithPort + '/storage/storages';
    this.storageTypesURL = this.serverWithPort + '/storageType/storageTypes';
    this.storageSystemsURL = this.serverWithPort + '/storageSystem/storageSystems';
    this.accessControlURL = this.serverWithPort + '/ranger/policiesByLocation';
    this.teradataAccessControlURL = this.serverWithPort + '/teradata/policy/$storageSystemId$/$databasename$';
    this.distinctObjectAttributesURL = this.serverWithPort + '/objectschema/distinctAttributeValues/';
    this.storageTypeURLForCategory = this.serverWithPort + '/storageType/storageTypeByStorageId/$storageId$';
    this.storageTypeURLForCategoryName = this.serverWithPort + '/storageType/storageTypeByStorageName/$storageName$';
    this.storageSystemURLForTypeName = this.serverWithPort + '/storageSystem/storageSystemByTypeName/$storageTypeName$';
    this.storageSystemDiscoveryURL = this.serverWithPort + '/storageSystem/storageSystemDiscovery/$storageSystemList$';
    this.storageSystemsURLForType = this.serverWithPort + '/storageSystem/storageSystemByType/$storageTypeId$';
    this.storageSystemURLForZoneName = this.serverWithPort + '/storageSystem/storageSystemByZoneName/$zoneName$/$typeName$';
    this.containersURLForSystem = this.serverWithPort + '/objectschema/containers/$storageSystemId$';
    this.containersURLForSystemList = this.serverWithPort + '/objectschema/containersBySystems/$storageSystemList$';
    this.containersURL = this.serverWithPort + '/objectschema/containers/';
    this.objectsForContainerAndSystem = this.serverWithPort + '/objectschema/objectnames/$containerName$/$storageSystemId$';
    this.storageTypeAttributesForType = this.serverWithPort + '/storageType/storageAttributeKey/$storageTypeId$/$isStorageSystemLevel$';
    this.userNameURL = this.serverWithPort + '/user/userByName/$username$';
    this.datasetPostURL = this.serverWithPort + '/dataSet/dataSet';
    this.teradataColumnDescriptionPutURL = this.serverWithPort + '/schemaIntegration/schemaDatasetColumn';
    this.teradataObjectDescriptionPostURL = this.serverWithPort + '/schemaIntegration/schemaFromUDC';
    this.teradataColumnDescriptionPostURL = this.serverWithPort + '/schemaIntegration/schemaColumnFromUDC';
    this.datasetObjectDescriptionPutURL = this.serverWithPort + '/description/description';
    this.datasetColumnDescriptionPostURL = this.serverWithPort + '/description/descriptionColumn';
    this.teradataObjectDescriptionPutURL = this.serverWithPort + '/schemaIntegration/schemaDataset';
    this.objectsPostURL = this.serverWithPort + '/objectschema/schema';
    this.objectAttributePostURL = this.serverWithPort + '/objectschema/customAttribute';
    this.clusterPostURL = this.serverWithPort + '/cluster/cluster';
    this.ownershipPostURL = this.serverWithPort + '/ownership/owner';
    this.datasetTagPostURL = this.serverWithPort + '/tag/tagForDataset';
    this.zonePostURL = this.serverWithPort + '/zone/zone';
    this.insertUserURL = this.serverWithPort + '/user/user';
    this.userPostURL = this.serverWithPort + '/user/user';
    this.categoryPostURL = this.serverWithPort + '/storage/storage';
    this.typePostURL = this.serverWithPort + '/storageType/storageType';
    this.typeAttributePostURL = this.serverWithPort + '/storageType/storageTypeAttribute';
    this.systemPostURL = this.serverWithPort + '/storageSystem/storageSystem';
    this.clustersByNameURL = this.serverWithPort + '/cluster/clusterByName/$clusterName$';
    this.clustersByIdURL = this.serverWithPort + '/cluster/cluster/$clusterId$';
    this.zoneByIdURL = this.serverWithPort + '/zone/zone/$zoneId$';
    this.teradataDescriptionByDatasetURL = this.serverWithPort + '/schemaIntegration/schema/$systemId$/$containerName$/$objectName$/$providerName$';
    this.teradataColumnDescriptionByDatasetURL = this.serverWithPort + '/schemaIntegration/schemaColumn/$schemaDatasetMapId$/$columnName$/$columnDatatype$';
    this.datasetColumnDescriptionByDatasetURL = this.serverWithPort + '/description/descriptionColumn/$bodhiDatasetMapId$/$columnName$';
    this.datasetDescriptionByDatasetIdURL = this.serverWithPort + '/description/description/$systemId$/$containerName$/$objectName$/$providerName$';
    this.deleteDatasetURL = this.serverWithPort + '/dataSet/dataSet/$datasetId$';
    this.deleteClusterURL = this.serverWithPort + '/cluster/dcluster/$clusterId$';
    this.deleteZoneURL = this.serverWithPort + '/zone/dzone/$zoneId$';
    this.deleteUserURL = this.serverWithPort + '/user/duser/$userId$';
    this.deleteCategoryURL = this.serverWithPort + '/storage/dstorage/$categoryId$';
    this.deleteTypeURL = this.serverWithPort + '/storageType/dstorageType/$typeId$';
    this.deleteSystemURL = this.serverWithPort + '/storageSystem/dstorageSystem/$storageSystemId$';
    this.enableClusterURL = this.serverWithPort + '/cluster/ecluster/$clusterId$';
    this.enableZoneURL = this.serverWithPort + '/zone/ezone/$zoneId$';
    this.enableCategoryURL = this.serverWithPort + '/storage/estorage/$categoryId$';
    this.enableUserURL = this.serverWithPort + '/user/euser/$userId$';
    this.enableTypeURL = this.serverWithPort + '/storageType/estorageType/$typeId$';
    this.enableSystemURL = this.serverWithPort + '/storageSystem/estorageSystem/$storageSystemId$';
    this.schemaByObjectIdURL = this.serverWithPort + '/objectschema/schema/$objectId$';
    this.systemAttributesByNameURL = this.serverWithPort + '/storageSystem/storageSystemAttributesByName/$systemName$';
    this.datasetPostAttributesURL = this.serverWithPort + '/dataSet/dataSetWithObjectAttributes';
    this.uniqueDatasetCountURL = this.serverWithPort + '/dataSet/totalUniqueCount';
    this.storageSystemURL = this.serverWithPort + '/storageSystem/storageSystem/$storageSystemId$';
    this.actionDBDetailsURL = `${ this.actionDetailsURL }/database/$DB$/$action$`;
    this.actionBackupDetailsURL = `${ this.actionDetailsURL }/backup/$backup$/$action$`;
    this.entitiesURL = this.serverWithPort + '/entity/entities';
    this.notificationURL = this.serverWithPort + '/notification/notifications';
    this.entityPostURL = this.serverWithPort + '/entity/entity';
    this.notificationPostURL = this.serverWithPort + '/notification/notification';
    this.notificationBulkUploadURL = this.serverWithPort + '/notification/notifications';
    this.notificationPutURL = this.serverWithPort + '/notification/notification';
    this.ownershipPostURL = this.serverWithPort + '/ownership/owner';
    this.classificationPostURL = this.serverWithPort + '/classification';
    this.entityByIdURL = this.serverWithPort + '/entity/entity/$entityId$';
    this.deleteEntityURL = this.serverWithPort + '/entity/dentity/$entityId$';
    this.enableEntityURL = this.serverWithPort + '/entity/eentity/$entityId$';
    this.changeLogByDatasetURL = this.serverWithPort + '/dataSet/changeLogs/$datasetId$';
    this.classificationURL = this.serverWithPort + '/classification/classifications';
    this.classificationURLByClassID = this.serverWithPort + '/classificationById/$classId$';
    this.classificationPostURL = this.serverWithPort + '/classification/';
    this.classificationBulkPostURL = this.serverWithPort + '/classification/bulk';
    this.classificationPutURL = this.serverWithPort + '/classification/';
    this.tableDatastoreMapURL = this.serverWithPort + '/objectschema/dataset/$objectName$';

  }

  getEntityList(): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.entitiesURL);
  }


  getNotificationPageList(): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.notificationURL);
  }

  getClassificationList(page: Page): Observable<Array<any>> {
    const pageNumber = page.pageNumber.toString();
    const size = page.size.toString();
    return this.api.get<any>(this.classificationURL.concat('?page=').concat(pageNumber).concat('&size=').concat(size));
  }

  private getPagedClassificationData(page: Page): PagedData<Classification> {
    const pagedData = new PagedData<Classification>();
    page.totalElements = this.classificationDetails.totalElements;
    page.totalPages = this.classificationDetails.totalPages;
    const numberOfElements = this.classificationDetails.numberOfElements;
    for (let i = 0; i < numberOfElements; i++) {
      const jsonObj = this.classificationDetails.content[i];
      const employee = new Classification(jsonObj.datasetClassificationId, jsonObj.classificationId, jsonObj.objectName, jsonObj.columnName, jsonObj.providerName, jsonObj.classificationComment, jsonObj.createdUser, jsonObj.updatedUser,  jsonObj.datasetIds);
      pagedData.data.push(employee);
    }
    pagedData.page = page;
    return pagedData;
  }

  getClassificationListPageable(page: Page): Observable<PagedData<Classification>> {
    return Observable.create(observer => {
      this.getClassificationList(page).subscribe(data => {
        this.classificationDetails = data;
        this.pagedClassificationDetails = this.getPagedClassificationData(page);
        observer.next(this.pagedClassificationDetails);
        observer.complete();
      });
    });
  }

  getClassificationListByClassID(classificationId: number, page: Page): Observable<Array<any>> {
    const pageNumber = page.pageNumber.toString();
    const size = page.size.toString();
    return this.api.get<any>(this.classificationURLByClassID.replace('$classId$', classificationId.toString()).concat('?page=').concat(pageNumber).concat('&size=').concat(size));
  }

  private getPagedClassificationDataByClassID(page: Page): PagedData<Classification> {
    const pagedData = new PagedData<Classification>();
    page.totalElements = this.classificationDetails.totalElements;
    page.totalPages = this.classificationDetails.totalPages;
    const numberOfElements = this.classificationDetails.numberOfElements;
    for (let i = 0; i < numberOfElements; i++) {
      const jsonObj = this.classificationDetails.content[i];
      const employee = new Classification(jsonObj.datasetClassificationId, jsonObj.classificationId, jsonObj.objectName, jsonObj.columnName, jsonObj.providerName, jsonObj.classificationComment, jsonObj.createdUser, jsonObj.updatedUser, jsonObj.datasetIds);
      pagedData.data.push(employee);
    }
    pagedData.page = page;
    return pagedData;
  }

  getClassificationListPageableByClassID(classificationId: number, page: Page): Observable<PagedData<Classification>> {
    return Observable.create(observer => {
      this.getClassificationListByClassID(classificationId, page).subscribe(data => {
        this.classificationDetails = data;
        this.pagedClassificationDetails = this.getPagedClassificationDataByClassID(page);
        observer.next(this.pagedClassificationDetails);
        observer.complete();
      });
    });
  }

  insertClassification(classification: Classification) {
    return this.api.post<any>(this.classificationPostURL, classification);
  }

  insertBulkClassification(classification: Array<Classification>) {
    return this.api.post<any>(this.classificationBulkPostURL, classification);
  }

  editClassification(classification: Classification) {
    return this.api.put<any>(this.classificationPutURL, classification);
  }

  getDatastoreMapingList(objectName: string): Observable<Array<any>> {
    return this.api.get<any>(this.tableDatastoreMapURL.replace('$objectName$', objectName));
  }

  getEntityById(entityId: string): Observable<any> {
    return this.api.get<any>(this.entityByIdURL.replace('$entityId$', entityId.toString()));
  }

  insertEntity(entity: Entity) {
    return this.api.post<any>(this.entityPostURL, entity);
  }

  insertNotification(notification: Notification) {
    return this.api.post<any>(this.notificationPostURL, notification);
  }

  bulkUploadNotification(notification: Notification) {
    return this.api.post<any>(this.notificationBulkUploadURL, notification);
  }

  insertDatasetDescription(datasetDescription: DatasetObjectDescription) {
    return this.api.post<any>(this.datasetObjectDescriptionPutURL, datasetDescription);
  }

  insertDatasetColumnDescription(datasetColumnDescription: DatasetDescription) {
    return this.api.post<any>(this.datasetColumnDescriptionPostURL, datasetColumnDescription);
  }

  updateDatasetColumnDescription(datasetColumnDescription: DatasetDescription) {
    return this.api.put<any>(this.datasetColumnDescriptionPostURL, datasetColumnDescription);
  }

  updateDatasetColumnDescriptionCurrent(datasetColumnDescription: DatasetComment) {
    return this.api.post<any>(this.datasetObjectDescriptionPutURL, datasetColumnDescription);
  }

  insertTeradataDescription(teradataDescription: TeradataObjectDescription) {
    return this.api.post<any>(this.teradataObjectDescriptionPostURL, teradataDescription);
  }

  insertTeradataColumnDescription(teradataColumnDescription: TeradataDescription) {
    return this.api.post<any>(this.teradataColumnDescriptionPostURL, teradataColumnDescription);
  }

  updateEntity(entity: Entity) {
    return this.api.put<any>(this.entityPostURL, entity);
  }

  updateNotification(notification: Notification) {
    return this.api.put<any>(this.notificationPutURL, notification);
  }

  updateOwnership(ownership: Ownership) {
    return this.api.post<any>(this.ownershipPostURL, ownership);
  }

  updateClassification(classification: Classification) {
    return this.api.post<any>(this.classificationPostURL, classification);
  }

  deleteEntity(entityId: string) {
    return this.api.delete<any>(this.deleteEntityURL.replace('$entityId$', entityId));
  }

  enableEntity(entityId: string) {
    return this.api.putWithoutPayload<any>(this.enableEntityURL.replace('$entityId$', entityId));
  }

  /**
   * This function gets all the storage categories which are supported.
   */
  getStorageCategories() {
    return this.api.get<Array<any>>(this.storageCategoriesURL);
  }

  /**
   * This function gets all the storage types which are supported.
   */
  getStorageTypes() {
    return this.api.get<Array<any>>(this.storageTypesURL);
  }

  getStorageTypesForCategory(storageCategoryId: string) {
    return this.api.get<Array<any>>(this.storageTypeURLForCategory.replace('$storageId$', storageCategoryId));
  }

  getStorageTypeAttributes(storageTypeId: string, isStorageSystemLevel: string) {
    return this.api.get<Array<any>>(this.storageTypeAttributesForType.replace('$storageTypeId$', storageTypeId).replace('$isStorageSystemLevel$', isStorageSystemLevel));
  }

  getStorageSystemsForType(storageTypeId: string) {
    return this.api.get<Array<any>>(this.storageSystemsURLForType.replace('$storageTypeId$', storageTypeId));
  }

  getSystemAttributes(storageSystemName: string) {
    return this.api.get<Array<any>>(this.systemAttributesByNameURL.replace('$systemName$', storageSystemName));
  }

  getUserByName(username: string) {
    return this.api.get<Array<any>>(this.userNameURL.replace('$username$', username));
  }

  getContainersForSystemList(storageSystemList: string) {
    return this.api.get<Array<any>>(this.containersURLForSystemList.replace('$storageSystemList$', storageSystemList));
  }

  getContainersForSystem(storageSystemId: string) {
    return this.api.get<Array<any>>(this.containersURLForSystem.replace('$storageSystemId$', storageSystemId));
  }

  getContainers() {
    return this.api.get<Array<any>>(this.containersURL);
  }

  getObjectsForContainerAndSystem(containerName: string, storageSystemId: string) {
    return this.api.get<Array<any>>(this.objectsForContainerAndSystem.replace('$storageSystemId$', storageSystemId).replace('$containerName$', containerName));
  }

  /**
   * This function gets all the storage categories which are supported.
   */
  getStorageSystems() {
    return this.api.get<Array<any>>(this.storageSystemsURL);
  }

  getStorageSystem(storageSystemId: string) {
    return this.api.get<any>(this.storageSystemURL.replace('$storageSystemId$', storageSystemId));
  }

  getAccessControl(location: string, type: string, clusterId: number, table: string) {
    return this.api.get<Array<any>>(this.accessControlURL + '?location=' + location + '&type=' + type + '&cluster=' + clusterId + '&table=' + table);
  }

  getAccessControlForHive(hiveDbName: string, hiveTableName: string, type: string, clusterId: number) {
    return this.api.get<Array<any>>(this.accessControlURL + '?database=' + hiveDbName + '&type=' + type + '&cluster=' + clusterId + '&table=' + hiveTableName);
  }

  getTeradataPolicy(storageSystemId: string, databasename: string) {
    return this.api.get<Array<any>>(this.teradataAccessControlURL.replace('$storageSystemId$', storageSystemId).replace('$databasename$', databasename));
  }

  getPendingDataSetList(datasetStr: string, systemName: string, typeName: string, page: Page): Observable<Array<any>> {
    const pageNumber = page.pageNumber.toString();
    const size = page.size.toString();
    return this.api.get<Array<any>>(this.dataSetsPendingURL.replace('$systemName$', systemName)
      .replace('$typeName$', typeName).concat('?datasetStr=').concat(datasetStr).concat('&page=').concat(pageNumber).concat('&size=').concat(size));
  }

  getPendingDataSetListPageable(datasetStr: string, systemName: string, typeName: string, page: Page): Observable<PagedData<Dataset>> {
    return Observable.create(observer => {
      this.getPendingDataSetList(datasetStr, systemName, typeName, page)
        .subscribe(data => {
          this.dataSetDetails = data;
          this.pagedData = this.getPagedData(page);
          observer.next(this.pagedData);
          observer.complete();
        }, error => {
          this.dataSetDetails = [];
        });
    });
  }

  getDeletedDataSetList(datasetStr: string, systemName: string, typeName: string, page: Page): Observable<Array<any>> {
    const pageNumber = page.pageNumber.toString();
    const size = page.size.toString();
    return this.api.get<Array<any>>(this.dataSetsDeletedURL.replace('$systemName$', systemName).replace('$typeName$', typeName).concat('?datasetStr=').concat(datasetStr).concat('&page=').concat(pageNumber).concat('&size=').concat(size));
  }

  getDeletedDataSetListPageable(datasetStr: string, systemName: string, typeName: string, page: Page): Observable<PagedData<Dataset>> {
    return Observable.create(observer => {
      this.getDeletedDataSetList(datasetStr, systemName, typeName, page)
        .subscribe(data => {
          this.dataSetDetails = data;
          this.pagedData = this.getPagedData(page);
          observer.next(this.pagedData);
          observer.complete();
        }, error => {
          this.dataSetDetails = [];
        });
    });
  }

  getDataSetList(datasetStr: string, containerName:string, systemName: string, typeName: string, page: Page, searchType: string): Observable<Array<any>> {
    const pageNumber = page.pageNumber.toString();
    const size = page.size.toString();
    if (environment.enableES) {
      return this.api.get<Array<any>>(this.dataSetsURLV2.concat('?storageSystemList=').concat(systemName).concat('&container=').concat(containerName).concat('&prefix=').concat(datasetStr).concat('&page=').concat(pageNumber).concat('&size=').concat(size).concat('&searchType=').concat(searchType));
    } else {
      return this.api.get<Array<any>>(this.dataSetsURL.replace('$systemName$', systemName).replace('$typeName$', typeName).concat('?datasetStr=').concat(datasetStr).concat('&page=').concat(pageNumber).concat('&size=').concat(size));
    }
  }

  getDataSetListPageable(datasetStr: string, containerName: string, systemName: string, typeName: string, page: Page, searchType: string): Observable<PagedData<Dataset>> {
    return Observable.create(observer => {
      this.getDataSetList(datasetStr, containerName, systemName, typeName, page, searchType)
        .subscribe(data => {
          this.dataSetDetails = data;
          this.pagedData = this.getPagedData(page);
          observer.next(this.pagedData);
          observer.complete();
        });
    });
  }

  private getPagedData(page: Page): PagedData<Dataset> {
    const pagedData = new PagedData<Dataset>();
    page.totalElements = this.dataSetDetails.totalElements;
    page.totalPages = this.dataSetDetails.totalPages;
    const numberOfElements = this.dataSetDetails.numberOfElements;
    for (let i = 0; i < numberOfElements; i++) {
      const jsonObj = this.dataSetDetails.content[i];
      const dataset = new Dataset(jsonObj.storageDataSetName, jsonObj.storageDataSetId, jsonObj.storageSystemName, jsonObj.attributesPresent, jsonObj.objectSchemaMapId, jsonObj.storageDataSetAliasName, jsonObj.storageDataSetDescription, jsonObj.createdUser, jsonObj.updatedUser, jsonObj.zoneName, jsonObj.isAccessControlled, jsonObj.teradataPolicies, jsonObj.derivedPolicies, jsonObj.isReadCompatible, jsonObj.isActiveYN, jsonObj.entityName);
      pagedData.data.push(dataset);
    }
    pagedData.page = page;
    return pagedData;
  }

  getObjectList(objectStr: string, systemName: string, page: Page): Observable<Array<any>> {
    const pageNumber = page.pageNumber.toString();
    const size = page.size.toString();
    return this.api.get<Array<any>>(this.objectListURL.replace('$systemName$', systemName).concat('?objectStr=').concat(objectStr).concat('&page=').concat(pageNumber).concat('&size=').concat(size));
  }

  private getPagedObjectData(page: Page): PagedData<ObjectSchemaMap> {
    const pagedData = new PagedData<ObjectSchemaMap>();
    page.totalElements = this.objectDetails.totalElements;
    page.totalPages = this.objectDetails.totalPages;
    const numberOfElements = this.objectDetails.numberOfElements;
    for (let i = 0; i < numberOfElements; i++) {
      const jsonObj = this.objectDetails.content[i];
      const employee = new ObjectSchemaMap(jsonObj.objectId, jsonObj.objectName, jsonObj.containerName, jsonObj.storageSystemId, jsonObj.createdUser);
      pagedData.data.push(employee);
    }
    pagedData.page = page;
    return pagedData;
  }

  getObjectListPageable(objectStr: string, systemName: string, page: Page): Observable<PagedData<ObjectSchemaMap>> {
    return Observable.create(observer => {
      this.getObjectList(objectStr, systemName, page).subscribe(data => {
        this.objectDetails = data;
        this.pagedObjectData = this.getPagedObjectData(page);
        observer.next(this.pagedObjectData);
        observer.complete();
      });
    });
  }

  getEsDataSetList(searchString: string, columnCriteria: string, systemIds: string, zoneIds: string, autoRegisterValues: string, page: Page): Observable<Array<any>> {
    const pageNumber = page.pageNumber.toString();
    const size = page.size.toString();
    return this.api.get<Array<any>>(this.esDatasetURL
      .concat('?searchQuery=').concat(searchString)
      .concat('&columnCriteria=').concat(columnCriteria)
      .concat('&systemIds=').concat(systemIds)
      .concat('&zoneIds=').concat(zoneIds)
      .concat('&autoRegisterOptions=').concat(autoRegisterValues)
      .concat('&page=').concat(pageNumber)
      .concat('&size=').concat(size));
  }

  getPagedEsDataSet(page: Page): PagedData<ElasticSearchDataset> {
    const pagedData = new PagedData<ElasticSearchDataset>();
    page.totalElements = this.esDataSetDetails.totalElements;
    page.totalPages = this.esDataSetDetails.totalPages;
    const numberOfElements = this.esDataSetDetails.numberOfElements;
    for (let i = 0; i < numberOfElements; i++) {
      const jsonObj = this.esDataSetDetails.content[i];
      const esDataSet = new ElasticSearchDataset(jsonObj.storageDataSetId, jsonObj.storageDataSetName, jsonObj.storageDataSetAliasName, jsonObj.storageDatabaseName, jsonObj.storageDataSetDescription, jsonObj.isActiveYN, jsonObj.userId, jsonObj.createdUser, jsonObj.createdTimestamp, jsonObj.updatedUser, jsonObj.updatedTimestamp, jsonObj.isAutoRegistered, jsonObj.objectSchemaMapId, jsonObj.storageSystemId, jsonObj.zoneId, jsonObj.isReadCompatible, jsonObj.containerName, jsonObj.objectName, jsonObj.isSelfDiscovered, jsonObj.isRegistered, jsonObj.createdUserOnStore, jsonObj.createdTimestampOnStore, jsonObj.objectSchema, jsonObj.objectAttributeValues);
      pagedData.data.push(esDataSet);
    }
    pagedData.page = page;
    return pagedData;
  }

  getEsDataSetListPageable(datasetStr: string, column: string, systemIds: string, zoneIds: string, autoRegisterValues: string, page: Page): Observable<PagedData<ElasticSearchDataset>> {
    return Observable.create(observer => {
      this.getEsDataSetList(datasetStr, column, systemIds, zoneIds, autoRegisterValues, page).subscribe(data => {
        this.esDataSetDetails = data;
        this.pagedEsData = this.getPagedEsDataSet(page);
        observer.next(this.pagedEsData);
        observer.complete();
      });
    });
  }

  getTypeList(storageName: string) {
    return this.api.get<Array<any>>(this.storageTypeURLForCategoryName.replace('$storageName$', storageName));
  }

  getSystemList(storageTypeName: string) {
    return this.api.get<Array<any>>(this.storageSystemURLForTypeName.replace('$storageTypeName$', storageTypeName));
  }

  getDiscoverySla(storageSystemList: string) {
    return this.api.get<Array<any>>(this.storageSystemDiscoveryURL.replace('$storageSystemList$', storageSystemList));
  }

  getSchemaDetails(objectId: string): Observable<any> {
    return this.api.get<any>(this.schemaByObjectIdURL.replace('$objectId$', objectId));
  }

  getDatasetPendingDetails(datasetId: string): Observable<any> {
    return this.api.get<any>(this.dataSetPendingURL.replace('$datasetId$', datasetId));
  }

  getDatasetDetailsByName(datasetName: string): Observable<any> {
    return this.api.get<any>(this.dataSetDetailsURL.replace('$datasetName$', datasetName));
  }

  getDatasetsWithPrefix(prefix: string): Observable<any> {
    return this.api.get<any>(this.datasetsPrefixUrl.concat('?prefixString=').concat(prefix));
  }

  getObjectDetails(objectId: string): Observable<any> {
    return this.api.get<any>(this.objectDetailsURL.replace('$objectId$', objectId));
  }

  getNotificationDetails(notificationId: string): Observable<any> {
    return this.api.get<any>(this.notificationDetailsURL.replace('$notificationId$', notificationId));
  }

  getCustomAttributes(objectId: string): Observable<any> {
    return this.api.get<any>(this.customAttributeURL.replace('$objectId$', objectId));
  }

  getClusterList(): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.clustersURL);
  }

  getNotificationList(): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.notificationsURL);
  }

  getZonesList(): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.zonesURL);
  }

  getZoneById(zoneId: string): Observable<any> {
    return this.api.get<any>(this.zoneByIdURL.replace('$zoneId$', zoneId.toString()));
  }

  getDescriptionForTeradata(systemId: string, containerName: string, objectName: string, providerName: string): Observable<any> {
    return this.api.get<any>(this.teradataDescriptionByDatasetURL.replace('$systemId$', systemId).replace('$containerName$', containerName).replace('$objectName$', objectName).replace('$providerName$', providerName));
  }

  getColumnDescriptionForTeradata(schemaDatasetMapId: string, columnName: string, columnDatatype: string) {
    return this.api.get<any>(this.teradataColumnDescriptionByDatasetURL.replace('$schemaDatasetMapId$', schemaDatasetMapId).replace('$columnName$', columnName).replace('$columnDatatype$', columnDatatype));
  }

  getColumnDescriptionForDataset(bodhiDatasetMapId: string, columnName: string) {
    return this.api.get<any>(this.datasetColumnDescriptionByDatasetURL.replace('$bodhiDatasetMapId$', bodhiDatasetMapId).replace('$columnName$', columnName));
  }

  getDescriptionForDataset(systemId: string, containerName: string, objectName: string, providerName: string): Observable<any> {
    return this.api.get<any>(this.datasetDescriptionByDatasetIdURL.replace('$systemId$', systemId).replace('$containerName$', containerName).replace('$objectName$', objectName).replace('$providerName$', providerName));
  }

  getSystemListForZoneAndType(zoneName: string, typeName: string) {
    return this.api.get<Array<any>>(this.storageSystemURLForZoneName.replace('$zoneName$', zoneName).replace('$typeName$', typeName));
  }

  getUsersList(): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.usersURL);
  }

  getClusterById(clusterId: number): Observable<any> {

    return this.api.get<any>(this.clustersByIdURL.replace('$clusterId$', clusterId.toString()));
  }

  getChangeLogByDatasetURL(datasetId: string): Observable<any> {

    return this.api.get<any>(this.changeLogByDatasetURL.replace('$datasetId$', datasetId));
  }

  getTypeAttributes(storageTypeId: string): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.typeAttributesByURL.replace('$storageTypeId$', storageTypeId));
  }

  getTypeAttributesAtSystemLevel(storageTypeId: string, isSystemLevel: string): Observable<Array<any>> {

    return this.api.get<Array<any>>(this.typeAttributesAtSystemLevelByURL.replace('$storageTypeId$', storageTypeId).replace('$isSystemLevel$', isSystemLevel));
  }

  getDatasetUniqueCount(): Observable<number> {
    return this.api.get<number>(this.uniqueDatasetCountURL);
  }

  insertTagForDataset(datasetTagMapInput: DatasetTagMapInput) {
    return this.api.post<any>(this.datasetTagPostURL, datasetTagMapInput);
  }

  insertCluster(cluster: Cluster) {
    return this.api.post<any>(this.clusterPostURL, cluster);
  }

  insertOwnership(datasetOwner: Ownership) {
    return this.api.post<any>(this.ownershipPostURL, datasetOwner);
  }

  insertZone(zone: Zone) {
    return this.api.post<any>(this.zonePostURL, zone);
  }

  insertDataset(payload: Dataset) {
    return this.api.post<any>(this.datasetPostURL, payload);
  }

  insertUser(user: User) {
    return this.api.post<any>(this.insertUserURL, user);
  }

  insertCategory(category: Category) {
    return this.api.post<any>(this.categoryPostURL, category);
  }

  insertType(type: Type) {
    return this.api.post<any>(this.typePostURL, type);
  }

  insertTypeAttribute(typeAttribute: StorageTypeAttribute) {
    return this.api.post<any>(this.typeAttributePostURL, typeAttribute);
  }

  insertSystem(system: System) {
    return this.api.post<any>(this.systemPostURL, system);
  }

  insertObject(objectSchema: ObjectSchemaMap) {
    return this.api.post<any>(this.objectsPostURL, objectSchema);
  }

  insertObjectAttribute(attribute: ObjectAttributeKeyValue) {
    return this.api.post<any>(this.objectAttributePostURL, attribute);
  }

  updateTeradataColumnDescription(payload: TeradataDescription) {
    return this.api.put<any>(this.teradataColumnDescriptionPutURL, payload);
  }

  updateTeradataObjectDescription(payload: TeradataObjectDescription) {
    return this.api.put<any>(this.teradataObjectDescriptionPutURL, payload);
  }

  updateDatasetObjectDescription(payload: DatasetObjectDescription) {
    return this.api.put<any>(this.datasetObjectDescriptionPutURL, payload);
  }

  updateDataset(payload: Dataset) {
    return this.api.put<any>(this.datasetPostURL, payload);
  }

  updateObject(payload: ObjectSchemaMap) {
    return this.api.put<any>(this.objectsPostURL, payload);
  }

  updateCustomAttributes(payload: ObjectAttributeKeyValue) {
    return this.api.put<any>(this.customAttributePutURL, payload);
  }

  updateDatasetWithAttributes(payload: DatasetWithAttributes) {
    return this.api.put<any>(this.datasetPostAttributesURL, payload);
  }

  updateCluster(cluster: Cluster) {
    return this.api.put<any>(this.clusterPostURL, cluster);
  }

  updateZone(zone: Zone) {
    return this.api.put<any>(this.zonePostURL, zone);
  }

  updateUser(user: User) {
    return this.api.put<any>(this.userPostURL, user);
  }

  updateCategory(category: Category) {
    return this.api.put<any>(this.categoryPostURL, category);
  }

  updateType(type: Type) {
    return this.api.put<any>(this.typePostURL, type);
  }

  updateSystem(system: System) {
    return this.api.put<any>(this.systemPostURL, system);

  }

  deleteDataset(datasetId: string) {
    return this.api.delete<any>(this.deleteDatasetURL.replace('$datasetId$', datasetId));
  }

  deleteCluster(clusterId: string) {
    return this.api.delete<any>(this.deleteClusterURL.replace('$clusterId$', clusterId));
  }

  deleteZone(zoneId: string) {
    return this.api.delete<any>(this.deleteZoneURL.replace('$zoneId$', zoneId));
  }

  deleteCategory(categoryId: string) {
    return this.api.delete<any>(this.deleteCategoryURL.replace('$categoryId$', categoryId));
  }

  deleteUser(userId: string) {
    return this.api.delete<any>(this.deleteUserURL.replace('$userId$', userId));
  }

  deleteType(typeId: string) {
    return this.api.delete<any>(this.deleteTypeURL.replace('$typeId$', typeId));
  }

  deleteSystem(systemId: string) {
    return this.api.delete<any>(this.deleteSystemURL.replace('$storageSystemId$', systemId));
  }

  enableCluster(clusterId: string) {
    return this.api.putWithoutPayload<any>(this.enableClusterURL.replace('$clusterId$', clusterId));
  }

  enableZone(zoneId: string) {
    return this.api.putWithoutPayload<any>(this.enableZoneURL.replace('$zoneId$', zoneId));
  }

  enableUser(userId: string) {
    return this.api.putWithoutPayload<any>(this.enableUserURL.replace('$userId$', userId));
  }

  enableCategory(categoryId: string) {
    return this.api.putWithoutPayload<any>(this.enableCategoryURL.replace('$categoryId$', categoryId));
  }

  enableType(typeId: string) {
    return this.api.putWithoutPayload<any>(this.enableTypeURL.replace('$typeId$', typeId));
  }

  enableSystem(systemId: string) {
    return this.api.putWithoutPayload<any>(this.enableSystemURL.replace('$storageSystemId$', systemId));
  }

  getTimelineDimensions(): Observable<Map<any, any>> {
    return this.api.get<Map<any, any>>(this.timelineListURL);
  }
}
