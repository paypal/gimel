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

import {Component} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {CatalogService} from '../../udc/catalog/services/catalog.service';
import {Dataset} from '../../udc/catalog/models/catalog-dataset';
import {Schema} from '../../udc/catalog/models/catalog-schema';
import {ConfigService} from '../../core/services/config.service';
import 'rxjs/add/observable/throw';
import {MatDialog, MatDialogConfig, MatDialogRef, ThemePalette} from '@angular/material';
import {MatChipInputEvent, MatTableDataSource} from '@angular/material';
import {COMMA, ENTER} from '@angular/cdk/keycodes';
import {DatasetTagMapInput} from '../../admin/models/catalog-dataset-tag-map-input';
import {BehaviorSubject} from 'rxjs/BehaviorSubject';
import {DataSource} from '@angular/cdk/collections';
import {Observable} from 'rxjs/Rx';
import {CatalogDescriptionEditDialogComponent} from './description-edit-dialog/catalog-description-edit-dialog.component';
import {MatSnackBar} from '@angular/material';
import {SessionService} from '../../core/services/session.service';
import {DatasetDescriptionDialogComponent} from '../../dataset-description-dialog/dataset-description-dialog.component';
import {CatalogOwnershipClaimDialogComponent} from './ownership-claim-dialog/catalog-ownership-claim-dialog.component';

export interface ChipColor {
  name: string;
  color: ThemePalette;
}

export class ExampleDataSource extends DataSource<any> {
  private dataSubject = new BehaviorSubject<Schema[]>([]);

  data() {
    return this.dataSubject.value;
  }

  update(data) {
    this.dataSubject.next(data);
  }

  constructor(data: any[]) {
    super();
    this.dataSubject.next(data);
  }

  connect(): Observable<Schema[]> {
    return this.dataSubject;
  }

  disconnect() {
  }
}

@Component({
  selector: 'app-standalonestage-profile', templateUrl: './standalone-dataset.component.html', styleUrls: ['./standalone-dataset.component.scss'],
})
export class StandaloneStageProfileComponent {

  // SCHEMA DETAILS RELATED VARIABLES
  private initialData: Schema[] = [];
  displayedColumns = ['columnName', 'columnType', 'partitionStatus', 'columnClassification', 'columnDescription'];
  dataSource: ExampleDataSource;

  update(el: Schema, comment: string) {
    if (comment == null) {
      return;
    }
    const copy = this.dataSource.data().slice();
    el.businessDescription = comment;
    this.dataSource.update(copy);
  }

  // BASIC DATASET VARIABLES
  datasetName = '';
  dataset: Dataset = new Dataset('', 0, '', null, 0, '', '', '', '', '', '', [], [], '', '', '');

  schemaColumns = ['columnName', 'columnType', 'partitionStatus', 'columnClassification', 'columnDescription'];
  systemAttrColumns = ['storageDsAttributeKeyName', 'storageSystemAttributeValue'];
  objectAttrColumns = ['storageDsAttributeKeyName', 'objectAttributeValue'];
  public dataSourceList = new MatTableDataSource();
  public systemAttributesList = new MatTableDataSource();
  public objectAttributesListTable = new MatTableDataSource();

  public detailsLoading = false;
  public columnList = new Array<Schema>();
  public columnDescriptionFromDart = [];
  public consolidatedColumnList = new Array<Schema>();
  public objectAttributesList = [];
  public accessControlList = [];
  public panelOpenState = true;
  public eventInfo: number;
  public tags = [];
  public retreivedTags = [];
  public dataList = [];
  public headersList = [];
  public rowsList = [];
  public dataLoading = false;
  public datasetname: string;
  public storageSystemId: number;
  public type: string;
  public columnClassificationMap = {};
  public objectDescription = '';
  public completeObjectDescription = '';
  public descriptionProviderName = '';
  public username = '';
  public inProgress = false;
  public providerName = 'USER';
  public existingOwners = '';
  public existingOwnerArray = [];
  messages = {
    emptyMessage: `
    <div>
      <span>Access control policies on this dataset are not available at the moment.</span>
    </div>`,
  };
  kafkaMessages = {
    emptyMessage: `
    <div>
      <span>This dataset has no access control policies implemented.</span>
    </div>`,
  };

  public defaultUserPermissionData = 'User does not have Read Permissions to this Dataset.';

  // TAG RELATED VARIABLES
  public availableColor: ChipColor = {name: 'Primary', color: 'primary'};
  tiles = [{text: 'One', cols: 2, rows: 1, color: 'lightblue'}, {text: 'Two', cols: 2, rows: 1, color: 'lightgreen'}, {text: 'Three', cols: 2, rows: 1, color: 'lightpink'}, {text: 'Four', cols: 2, rows: 1, color: '#DDBDF1'}];
  visible = true;
  selectable = true;
  removable = true;
  addOnBlur = true;
  readonly separatorKeysCodes: number[] = [ENTER, COMMA];

  dialogViewConfig: MatDialogConfig = {width: '1000px', height: '90vh'};

  constructor(private config: ConfigService, private snackbar: MatSnackBar, private route: ActivatedRoute, private catalogService: CatalogService, private dialog: MatDialog, private sessionService: SessionService) {
    this.config.getUserNameEmitter().subscribe(data => {
      this.username = data;
    });
    this.username = this.config.userName;

    this.columnClassificationMap['restricted_columns_class1'] = 'Class 1';
    this.columnClassificationMap['restricted_columns_class2'] = 'Class 2';
    this.columnClassificationMap['restricted_columns_class3_1'] = 'Class 3';
    this.columnClassificationMap['restricted_columns_class3_2'] = 'Class 3';
    this.columnClassificationMap['restricted_columns_class4'] = 'Class 4';
    this.columnClassificationMap['restricted_columns_class5'] = 'Class 5';
    this.columnClassificationMap[''] = 'N/A';
    this.getDatasetDetails();
  }

  getAccessControls(storageSystemId: number) {
    this.catalogService.getStorageSystem(storageSystemId.toString()).subscribe(data => {
      this.type = data.storageType.storageTypeName;
      const clusterId: number = data.runningClusterId;
      if (this.type === 'Hive') {
        let tempHiveTableName = '';
        let tempHiveDbName = '';
        this.catalogService.getClusterById(clusterId).subscribe(clusterData => {
          let listnum = 0;
          for (listnum = 0; listnum < this.objectAttributesList.length; listnum++) {
            const stringToReplace = 'hdfs://' + clusterData.clusterName.toLowerCase();
            const attribute = this.objectAttributesList[listnum];
            const attributeValue = attribute.objectAttributeValue.replace(stringToReplace, '');
            if (attributeValue.includes('/')) {
              this.getAccessForHadoop(attributeValue, 'hdfs', clusterId, '');
            } else if (attribute.storageDsAttributeKeyName.includes('db')) {
              tempHiveDbName = attributeValue;
            } else if (attribute.storageDsAttributeKeyName.includes('table')) {
              tempHiveTableName = attributeValue;
            }
          }
          if (tempHiveTableName.length > 0 && tempHiveDbName.length > 0) {
            this.getAccessForHive(tempHiveDbName, tempHiveTableName, 'hive', clusterId);
          }
        }, error1 => {
        });
      } else if (this.type === 'Hbase') {
        let listnum = 0;
        for (listnum = 0; listnum < this.objectAttributesList.length; listnum++) {
          const attributeValue = this.objectAttributesList[listnum].objectAttributeValue;
          const attributeValuesArray = attributeValue.split(':');
          if (attributeValue.includes(':') && attributeValuesArray.length === 2 && attributeValuesArray.indexOf('') === -1) {
            this.getAccessForHadoop('', this.type.toLowerCase(), clusterId, attributeValue);
          }
        }
      } else if (this.type === 'Teradata') {
        let listnum = 0;
        for (listnum = 0; listnum < this.objectAttributesList.length; listnum++) {
          const attributeValue = this.objectAttributesList[listnum].objectAttributeValue;
          const databaseName = attributeValue.split('.')[0];
          this.getAccessForTeradata(storageSystemId, databaseName);

        }
      }
    }, error => {
    });

  }

  getAccessForTeradata(storageSystemId: number, attributeValue: string) {
    this.catalogService.getTeradataPolicy(storageSystemId.toString(), attributeValue).subscribe(data => {
      if (data && data.length > 0) {
        data.forEach(element => {
          this.accessControlList.push(element);
        });
        this.accessControlList = [...this.accessControlList];
      }
    }, error => {
    });
  }

  getAccessForHive(hiveDbName: string, hiveTableName: string, type: string, clusterId: number) {
    let tempList = [];
    this.catalogService.getAccessControlForHive(hiveDbName, hiveTableName, type, clusterId).subscribe(data => {
      if (data && data.length > 0) {
        tempList = data;
        let num = 0;
        for (num = 0; num < tempList.length; num++) {
          const temp = tempList[num];
          const policies = temp.policyItems;
          policies.forEach(policy => {
            this.accessControlList.push(policy);
          });
        }
        this.accessControlList = [...this.accessControlList];
      }
    }, error => {
    });
  }

  getAccessForHadoop(attributeValue: string, type: string, clusterId: number, table: string) {
    let tempList = [];
    this.catalogService.getAccessControl(attributeValue, type, clusterId, table).subscribe(data => {
      if (data && data.length > 0) {
        tempList = data;
        let num = 0;
        for (num = 0; num < tempList.length; num++) {
          const temp = tempList[num];
          const policies = temp.policyItems;
          policies.forEach(policy => {
            this.accessControlList.push(policy);
          });
        }
        this.accessControlList = [...this.accessControlList];
      }
    }, error => {
    });
  }

  copyText(val: string) {
    let selBox = document.createElement('textarea');
    selBox.style.position = 'fixed';
    selBox.style.left = '0';
    selBox.style.top = '0';
    selBox.style.opacity = '0';
    selBox.value = val;
    document.body.appendChild(selBox);
    selBox.focus();
    selBox.select();
    document.execCommand('copy');
    document.body.removeChild(selBox);
  }

  copyUrl() {
    this.copyText(window.location.href);
  }

  getDatasetDetails() {
    this.route.params.subscribe(params => {
      this.detailsLoading = true;
      this.dataLoading = true;
      this.datasetName = params['datasetName'];
      this.accessControlList = [];
      this.columnList = [];
      this.columnDescriptionFromDart = [];
      this.tags = [];
      this.consolidatedColumnList = [];

      const tempMap = new Map();
      this.catalogService.getDatasetDetailsByName(this.datasetName.toString())
        .subscribe(data => {
          this.dataset = data;
          if (data.isAutoRegistered === 'N') {
            this.dataset.isAutoRegistered = 'This Dataset has been discovered by the UDC Discovery Services. Please contact #help-udc for further details.';
          } else {
            this.dataset.isAutoRegistered = 'This Dataset has been created by ' + this.dataset.createdUser + '. Please contact ' + this.dataset.createdUser + ' for further details.';
          }
          this.detailsLoading = false;
          const tempList = data.objectSchema;
          if (tempList) {
            tempList.forEach(column => {
              const schemaObject = new Schema();
              schemaObject.columnClass = this.columnClassificationMap[column.columnClass];
              schemaObject.columnFamily = column.columnFamily;
              schemaObject.columnIndex = column.columnIndex;
              schemaObject.columnType = column.columnType;
              schemaObject.restrictionStatus = column.restrictionStatus;
              schemaObject.partitionStatus = column.partitionStatus;
              schemaObject.columnName = column.columnName;
              this.columnList.push(schemaObject);
            });
          }

          this.systemAttributesList = data.systemAttributes;
          this.objectAttributesList = data.objectAttributes;
          if (data.owners.length > 0) {
            this.existingOwners = data.owners.map(owner => owner.ownerName).join(',');
            this.existingOwnerArray = data.owners;
          } else {
            this.existingOwners = '';
          }

          // schema description present in the response or not
          if (data.schemaDescription) {
            // object description assignment
            this.completeObjectDescription = data.schemaDescription.datasetComment;
            // provider name for the entire description
            this.descriptionProviderName = data.schemaDescription.providerName;
            // trim the description to showcase on the UI
            if (this.completeObjectDescription) {
              this.objectDescription = this.completeObjectDescription.substring(0, 200).trim();
            }
            // assigning column descriptions from the payload
            this.columnDescriptionFromDart = data.schemaDescription.datasetColumnDescriptions;
            // column Names from dart or any 3rd party tool
            const columnNamesFromDart = this.columnDescriptionFromDart.map(column => column.columnName);
            // populate the map with names for look up
            this.columnDescriptionFromDart.forEach(column => {
              tempMap.set(column.columnName, column);
            });
            this.columnList.forEach(column => {
              if (columnNamesFromDart.indexOf(column.columnName) > -1) {
                const dartColumn = tempMap.get(column.columnName);
                column.businessDescription = dartColumn.columnComment;
                column.updatedTimestamp = dartColumn.updatedTimestamp;
              } else {
                column.businessDescription = 'N/A';
                column.updatedTimestamp = 'N/A';
              }
              this.consolidatedColumnList.push(column);
            });
          } else {
            this.objectDescription = 'N/A';
            this.columnList.forEach(column => {
              column.businessDescription = '';
              column.updatedTimestamp = '';
              this.consolidatedColumnList.push(column);
            });
          }
          if (data.tags && data.tags.length > 0) {
            data.tags.forEach(tag => {
              this.retreivedTags.push(tag);
              this.tags.push({name: tag.tagName, createdUser: tag.createdUser, createdTimestamp: tag.createdTimestamp});
            });
          }

          this.initialData = this.consolidatedColumnList;
          this.dataSource = new ExampleDataSource(this.initialData);
          this.dataSourceList = new MatTableDataSource(this.initialData);
          this.systemAttributesList = new MatTableDataSource(data.systemAttributes);
          this.objectAttributesListTable = new MatTableDataSource(data.objectAttributes);

          if (data.customAttributes) {
            data.customAttributes.forEach(attribute => {
              const key = attribute.objectAttributeKey;
              const value = attribute.objectAttributeValue;
              const objectAttribute = {'storageDsAttributeKeyName': key, 'objectAttributeValue': value};
              this.objectAttributesList.push(objectAttribute);
            });
          }


          this.datasetname = this.dataset.storageDataSetName;
          this.storageSystemId = data.storageSystemId;
          this.getAccessControls(this.storageSystemId);
          this.detailsLoading = false;
        }, error => {
          this.dataset = new Dataset('', 0, '', null, 0, '', '', '', '', '', '', [], [], '', '', '');
          this.detailsLoading = false;
        });
    });
  }

  eventCountHandler($event: number) {
    this.eventInfo = $event;
  }

  applyFilter(filterValue: string) {
    this.dataSourceList.filter = filterValue.trim().toLowerCase();
  }

  sysAttributeFilter(filterValue: string) {
    this.systemAttributesList.filter = filterValue.trim().toLowerCase();
  }

  objAttributeFilter(filterValue: string) {
    this.objectAttributesListTable.filter = filterValue.trim().toLowerCase();
  }

  saveTags() {
    this.inProgress = true;
    const retreivedTagNames = this.retreivedTags.map(tag => tag.tagName);
    const newTags = this.tags.filter(tag => retreivedTagNames.indexOf(tag.name) < 0);
    if (newTags.length > 0) {
      const tagsToPost = new Array<DatasetTagMapInput>();
      newTags.forEach(tag => {
        const datasetTagMapInput = new DatasetTagMapInput(tag.name, this.providerName, this.username, this.dataset.storageDataSetId);
        tagsToPost.push(datasetTagMapInput);
      });

      tagsToPost.forEach(tag => {
        this.catalogService.insertTagForDataset(tag).subscribe(data => {
        }, error => {
          this.inProgress = false;
        });
      });
      this.inProgress = false;
    } else {
      this.inProgress = false;
    }
  }

  claimOwnership() {

    const distinct = (value: any, index: any, self: any) => {
      return self.indexOf(value) === index;
    };
    let dialogRef: MatDialogRef<CatalogOwnershipClaimDialogComponent>;
    dialogRef = this.dialog.open(CatalogOwnershipClaimDialogComponent, this.dialogViewConfig);
    dialogRef.componentInstance.ownerName = this.username;
    dialogRef.componentInstance.ownerEmail = this.username + '@paypal.com';
    dialogRef.componentInstance.storageSystemName = this.dataset.storageSystemName;
    dialogRef.componentInstance.storageSystemId = this.storageSystemId;
    dialogRef.componentInstance.objectId = this.dataset.objectSchemaMapId;
    dialogRef.componentInstance.datasetId = this.dataset.storageDataSetId;
    dialogRef.componentInstance.otherOwners = this.existingOwnerArray.length > 0 ? this.existingOwnerArray.map(owner => owner.ownerName).filter(owner => owner !== this.username) : [];

    if (this.existingOwnerArray.length > 0) {
      let tempArray = [];
      this.existingOwnerArray.forEach(owner => {
        tempArray = tempArray.concat(owner.emailIlist.split(','));
      });
      tempArray = tempArray.filter(distinct);
      dialogRef.componentInstance.existingEmailIlist = tempArray;
    } else {
      dialogRef.componentInstance.existingEmailIlist = [];
    }
    dialogRef.componentInstance.existingComment = this.existingOwnerArray.length > 0 ? this.existingOwnerArray.map(owner => owner.ownershipComment)[0] : '';
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            const response = result.response.map(entry => entry);
            this.existingOwnerArray = [];
            response.forEach(entry => this.existingOwnerArray.push(entry));
            this.existingOwners = this.existingOwnerArray.map(owner => owner.ownerName).filter(distinct).join(',');
            this.snackbar.open('Claimed Ownership for the dataset with Ownership IDS -> ' + result.datasetOwnershipMapId, 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '. Failed to claim ownership', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '. Failed to claim Ownership', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  add(event: MatChipInputEvent) {
    const input = event.input;
    const value = event.value;
    if ((value || '').trim()) {
      this.tags.push({name: value.trim()});
    }
    if (input) {
      input.value = '';
    }
  }

  remove(tag): void {
    const index = this.tags.indexOf(tag);
    if (index >= 0) {
      this.tags.splice(index, 1);
    }
  }

  openFullView() {
    let dialogRef: MatDialogRef<DatasetDescriptionDialogComponent>;
    dialogRef = this.dialog.open(DatasetDescriptionDialogComponent, this.dialogViewConfig);
    dialogRef.componentInstance.datasetName = this.dataset.storageDataSetName;
    dialogRef.componentInstance.datasetDescription = this.completeObjectDescription;
    dialogRef.afterClosed().subscribe();
  }

  editObjectDescription() {
    let dialogRef: MatDialogRef<CatalogDescriptionEditDialogComponent>;
    dialogRef = this.dialog.open(CatalogDescriptionEditDialogComponent, this.dialogViewConfig);
    dialogRef.componentInstance.updatedUser = this.username;
    dialogRef.componentInstance.objectDescription = this.completeObjectDescription;
    dialogRef.componentInstance.datasetId = this.dataset.storageDataSetId;
    dialogRef.componentInstance.datasetName = this.dataset.storageDataSetName;
    dialogRef.componentInstance.systemName = this.dataset.storageSystemName;
    dialogRef.componentInstance.systemId = this.dataset.storageSystemId;
    dialogRef.componentInstance.objectId = this.dataset.objectSchemaMapId;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.completeObjectDescription = result.description;
            this.objectDescription = this.completeObjectDescription.substring(0, 200).trim();
            this.snackbar.open('Updated the Description with dataset ID -> ' + this.dataset.storageDataSetId, 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update description', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update description', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });

  }

}
