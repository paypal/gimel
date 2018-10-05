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
  public isGimelCompatible: string;
  public isReadCompatible: string;
  public zoneName: string;
  public isAccessControlled: string;
  public teradataPolicies = new Array<TeradataPolicy>();
  public derivedPolicies = new Array<DerivedPolicy>();

  constructor(storageDataSetName: string, storageDataSetId: number, storageSystemName: string, isAttributesPresent: boolean, objectSchemaMapId: number, storageDataSetAliasName: string, storageDataSetDescription: string, createdUser: string, updatedUser: string, isGimelCompatible: string, zoneName: string, isAccessControlled: string, teradataPolicies: Array<TeradataPolicy>, derivedPolicies: Array<DerivedPolicy>, isReadCompatible: string) {
    this.storageDataSetName = storageDataSetName;
    this.storageDataSetId = storageDataSetId;
    this.storageSystemName = storageSystemName;
    this.attributesPresent = isAttributesPresent;
    this.objectSchemaMapId = objectSchemaMapId;
    this.storageDataSetAliasName = storageDataSetAliasName;
    this.storageDataSetDescription = storageDataSetDescription;
    this.createdUser = createdUser;
    this.updatedUser = updatedUser;
    this.isGimelCompatible = isGimelCompatible;
    this.zoneName = zoneName;
    this.isAccessControlled = isAccessControlled;
    this.teradataPolicies = teradataPolicies;
    this.derivedPolicies = derivedPolicies;
    this.isReadCompatible = isReadCompatible;
  }
}
