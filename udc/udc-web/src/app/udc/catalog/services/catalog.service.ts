import {Injectable} from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {ApiService} from '../../../core/services';
import {Dataset} from '../models/catalog-dataset';
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

@Injectable()
export class CatalogService {
  private datasetPostAttributesURL: any;
  private actionDetailsURL: string;
  private actionDBDetailsURL: string;
  private actionBackupDetailsURL: string;
  private dataSetsURL: string;
  private objectListURL: string;
  private dataSetsPendingURL: string;
  private dataSetsDeletedURL: string;
  private dataSetURL: string;
  private dataSetPendingURL: string;
  private datasetsPrefixUrl: string;
  private sampleDataURL: string;
  private objectDetailsURL: string;
  private clustersURL: string;
  private zonesURL: string;
  private usersURL: string;
  private storageCategoriesURL: string;
  private storageTypesURL: string;
  private storageSystemsURL: string;
  private accessControlURL: string;
  private teradataAccessControlURL: string;
  private storageTypeURLForCategory: string;
  private storageTypeURLForCategoryName: string;
  private storageSystemURLForTypeName: string;
  private storageSystemsURLForType: string;
  private containersURLForSystem: string;
  private storageSystemURL: string;
  private containersURL: string;
  private objectsForContainerAndSystem: string;
  private storageTypeAttributesForType: string;
  private userNameURL: string;
  private datasetPostURL: string;
  private objectsPostURL: string;
  public clustersByNameURL: string;
  public deleteDatasetURL: string;
  private deleteTypeURL: string;
  private deleteSystemURL: string;
  private enableTypeURL: string;
  private enableSystemURL: string;
  public serverWithPort: string;
  private clusterPostURL: string;
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
  private objectDetails: any;
  private clustersByIdURL: string;
  private pagedObjectData = new PagedData<ObjectSchemaMap>();
  private pagedData = new PagedData<Dataset>();

  constructor(private api: ApiService) {
    this.serverWithPort = api.serverWithPort;
    const urlGenerator = (url) => {
      return `${this.serverWithPort}${url}`;
    };

    this.dataSetsURL = urlGenerator('/dataSet/dataSetsBySystem/$typeName$/$systemName$');
    this.objectListURL = urlGenerator('/objectschema/objects/$systemName$/$containerName$');
    this.typeAttributesByURL = urlGenerator('/storageType/storageAttributeKey/$storageTypeId$');
    this.typeAttributesAtSystemLevelByURL = urlGenerator('/storageType/storageAttributeKey/$storageTypeId$/$isSystemLevel$');
    this.dataSetsPendingURL = this.serverWithPort + '/dataSet/dataSetsBySystemPending/$typeName$/$systemName$';
    this.dataSetsDeletedURL = this.serverWithPort + '/dataSet/dataSetsBySystemDeleted/$typeName$/$systemName$';
    this.dataSetURL = this.serverWithPort + '/dataSet/dataSet/$datasetId$';
    this.dataSetPendingURL = this.serverWithPort + '/dataSet/dataSetPending/$datasetId$';
    this.datasetsPrefixUrl = this.serverWithPort + '/dataSet/detailedDataSets/';
    this.sampleDataURL = this.serverWithPort + '/dataSet/sampleData/$dataset$/$objectId$';
    this.objectDetailsURL = this.serverWithPort + '/objectschema/schema/$objectId$';
    this.clustersURL = this.serverWithPort + '/cluster/clusters';
    this.zonesURL = this.serverWithPort + '/zone/zones';
    this.usersURL = this.serverWithPort + '/user/users';
    this.storageCategoriesURL = this.serverWithPort + '/storage/storages';
    this.storageTypesURL = this.serverWithPort + '/storageType/storageTypes';
    this.storageSystemsURL = this.serverWithPort + '/storageSystem/storageSystems';
    this.accessControlURL = this.serverWithPort + '/ranger/policiesByLocation';
    this.teradataAccessControlURL = this.serverWithPort + '/teradata/policy/$storageSystemId$/$databasename$';
    this.storageTypeURLForCategory = this.serverWithPort + '/storageType/storageTypeByStorageId/$storageId$';
    this.storageTypeURLForCategoryName = this.serverWithPort + '/storageType/storageTypeByStorageName/$storageName$';
    this.storageSystemURLForTypeName = this.serverWithPort + '/storageSystem/storageSystemByTypeName/$storageTypeName$';
    this.storageSystemsURLForType = this.serverWithPort + '/storageSystem/storageSystemByType/$storageTypeId$';
    this.containersURLForSystem = this.serverWithPort + '/objectschema/containers/$storageSystemId$';
    this.containersURL = this.serverWithPort + '/objectschema/containers/';
    this.objectsForContainerAndSystem = this.serverWithPort + '/objectschema/objectnames/$containerName$/$storageSystemId$';
    this.storageTypeAttributesForType = this.serverWithPort + '/storageType/storageAttributeKey/$storageTypeId$/$isStorageSystemLevel$';
    this.userNameURL = this.serverWithPort + '/user/userByName/$username$';
    this.datasetPostURL = this.serverWithPort + '/dataSet/dataSet';
    this.objectsPostURL = this.serverWithPort + '/objectschema/schema';
    this.clusterPostURL = this.serverWithPort + '/cluster/cluster';
    this.zonePostURL = this.serverWithPort + '/zone/zone';
    this.insertUserURL = this.serverWithPort + '/user/user';
    this.userPostURL = this.serverWithPort + '/user/user';
    this.categoryPostURL = this.serverWithPort + '/storage/storage';
    this.typePostURL = this.serverWithPort + '/storageType/storageType';
    this.typeAttributePostURL = this.serverWithPort + '/storageType/storageTypeAttribute';
    this.systemPostURL = this.serverWithPort + '/storageSystem/storageSystem';
    this.clustersByNameURL = this.serverWithPort + '/cluster/clusterByName/$clusterName$';
    this.clustersByIdURL = this.serverWithPort + '/cluster/cluster/$clusterId$';
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
    this.storageSystemURL = this.serverWithPort + '/storageSystem/storageSystem/$storageSystemId$';
    this.actionDBDetailsURL = `${ this.actionDetailsURL }/database/$DB$/$action$`;
    this.actionBackupDetailsURL = `${ this.actionDetailsURL }/backup/$backup$/$action$`;
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

  getAccessControl(location: string, type: string, clusterId: number) {
    return this.api.get<Array<any>>(this.accessControlURL + '?location=' + location + '&type=' + type + '&cluster=' + clusterId);
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

  /**
   * This function returns list of all datasets for storage system ID
   */
  getDataSetList(datasetStr: string, systemName: string, typeName: string, page: Page): Observable<Array<any>> {
    const pageNumber = page.pageNumber.toString();
    const size = page.size.toString();
    return this.api.get<Array<any>>(this.dataSetsURL.replace('$systemName$', systemName).replace('$typeName$', typeName)
      .concat('?datasetStr=').concat(datasetStr).concat('&page=').concat(pageNumber).concat('&size=').concat(size));
  }

  getDataSetListPageable(datasetStr: string, systemName: string, typeName: string, page: Page): Observable<PagedData<Dataset>> {
    return Observable.create(observer => {
      this.getDataSetList(datasetStr, systemName, typeName, page)
        .subscribe(data => {
          this.dataSetDetails = data;
          this.pagedData = this.getPagedData(page);
          observer.next(this.pagedData);
          observer.complete();
        });
    });
    // return Observable.of(this.pagedData);

  }

  private getPagedData(page: Page): PagedData<Dataset> {
    const pagedData = new PagedData<Dataset>();
    page.totalElements = this.dataSetDetails.totalElements;
    page.totalPages = this.dataSetDetails.totalPages;
    const numberOfElements = this.dataSetDetails.numberOfElements;
    for (let i = 0; i < numberOfElements; i++) {
      const jsonObj = this.dataSetDetails.content[i];
      const employee = new Dataset(jsonObj.storageDataSetName, jsonObj.storageDataSetId, jsonObj.storageSystemName, jsonObj.attributesPresent, jsonObj.objectSchemaMapId, jsonObj.storageDataSetAliasName, jsonObj.storageDataSetDescription, jsonObj.createdUser, jsonObj.updatedUser, jsonObj.isGimelCompatible, jsonObj.zoneName, jsonObj.isAccessControlled, jsonObj.teradataPolicies, jsonObj.derivedPolicies, jsonObj.isReadCompatible);
      pagedData.data.push(employee);
    }
    pagedData.page = page;
    return pagedData;
  }

  getObjectList(systemName: string, containerName: string, page: Page): Observable<Array<any>> {
    const pageNumber = page.pageNumber.toString();
    const size = page.size.toString();
    return this.api.get<Array<any>>(this.objectListURL.replace('$systemName$', systemName).replace('$containerName$', containerName).concat('?page=').concat(pageNumber).concat('&size=').concat(size));
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

  getObjectListPageable(systemName: string, containerName: string, page: Page): Observable<PagedData<ObjectSchemaMap>> {
    return Observable.create(observer => {
      this.getObjectList(systemName, containerName, page).subscribe(data => {
        this.objectDetails = data;
        this.pagedObjectData = this.getPagedObjectData(page);
        observer.next(this.pagedObjectData);
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

  getSchemaDetails(objectId: string): Observable<any> {
    return this.api.get<any>(this.schemaByObjectIdURL.replace('$objectId$', objectId));
  }

  /**
   * This function invokes UDC get Dataset API and returns current Dataset Details
   * @param datasetId
   */
  getDatasetPendingDetails(datasetId: string): Observable<any> {
    return this.api.get<any>(this.dataSetPendingURL.replace('$datasetId$', datasetId));
  }

  getDatasetsWithPrefix(prefix: string): Observable<any> {
    return this.api.get<any>(this.datasetsPrefixUrl.concat('?prefixString=').concat(prefix));
  }
  getSampleData(dataset: string, objectSchemaMapId: string): Observable<any> {
    return this.api.get<any>(this.sampleDataURL.replace('$dataset$', dataset).replace('$objectId$', objectSchemaMapId));
  }

  getObjectDetails(objectId: string): Observable<any> {
    return this.api.get<any>(this.objectDetailsURL.replace('$objectId$', objectId));
  }

  getClusterList(): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.clustersURL);
  }

  getZonesList(): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.zonesURL);
  }

  getUsersList(): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.usersURL);
  }

  getClusterById(clusterId: number): Observable<any> {

    return this.api.get<any>(this.clustersByIdURL.replace('$clusterId$', clusterId.toString()));
  }

  getTypeAttributes(storageTypeId: string): Observable<Array<any>> {
    return this.api.get<Array<any>>(this.typeAttributesByURL.replace('$storageTypeId$', storageTypeId));
  }

  getTypeAttributesAtSystemLevel(storageTypeId: string, isSystemLevel: string): Observable<Array<any>> {

    return this.api.get<Array<any>>(this.typeAttributesAtSystemLevelByURL.replace('$storageTypeId$', storageTypeId).replace('$isSystemLevel$', isSystemLevel));
  }

  insertCluster(cluster: Cluster) {
    return this.api.post<any>(this.clusterPostURL, cluster);
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

  updateDataset(payload: Dataset) {
    return this.api.put<any>(this.datasetPostURL, payload);
  }

  updateObject(payload: ObjectSchemaMap) {
    return this.api.put<any>(this.objectsPostURL, payload);
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
}
