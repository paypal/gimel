export class DatasetWithAttributes {
  public storageDataSetId: number;
  public storageDataSetName: string;
  public storageDatasetStatus: string;
  public objectSchemaMapId: number;
  public isAutoRegistered: string;
  public createdUser: string;
  public createdTimestamp: string;
  public updatedUser: string;
  public updatedTimestamp: string;
  public storageSystemId: number;
  public query: string;
  public objectSchema : Array<any>;
  public storageSystemName: string;
  public systemAttributes: Array<any>;
  public objectAttributes: Array<any>;
  public pendingTypeAttributes: Array<any>;
}
