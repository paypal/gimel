
export class System {
  public storageSystemId: number;
  public storageSystemName: string;
  public storageSystemDescription: string;
  public storageTypeId: number;
  public isActiveYN: string;
  public createdUser: string;
  public updatedUser: string;
  public containers: string;
  public systemAttributeValues: Array<any>;
  public adminUserId: number;
  public assignedClusterId: number;
  public runningClusterId: number;
  public zoneId: number;
  public zoneName: string;
  public isGimelCompatible: string;
  public isReadCompatible: string;
}
