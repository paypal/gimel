import {DatasetCluster} from './catalog-dataset-cluster';
export class ObjectSchemaMap {
  public objectId: number;
  public storageSystemId: number;
  public storageSystemName: string;
  public containerName: string;
  public objectName: string;
  public isActiveYN: string;
  public objectSchema: Array<any>;
  public objectAttributes: Array<any>;
  public updatedUser: string;
  public createdUser: string;
  public clusters: Array<any>;
  public isSelfDiscovered: string;


  constructor(objectId: number, objectName: string, containerName: string, storageSystemId: number, createdUser: string) {
    this.objectId = objectId;
    this.objectName = objectName;
    this.containerName = containerName;
    this.storageSystemId = storageSystemId;
    this.createdUser = createdUser;
  }


}
