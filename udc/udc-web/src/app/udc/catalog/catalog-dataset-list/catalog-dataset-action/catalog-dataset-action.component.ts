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

import { Component, Input, Output, EventEmitter } from '@angular/core';
import { forkJoin } from 'rxjs/observable/forkJoin';
import { MatSnackBar } from '@angular/material';
import {
  MatDialog, MatDialogRef, MatDialogConfig,
} from '@angular/material';

import { CatalogService } from '../../services/catalog.service';
import { ConfigService } from '../../../../core/services';
import { CatalogDatasetEditDialogComponent } from '../catalog-dataset-edit-dialog/catalog-dataset-edit-dialog.component';
import { CatalogObjectEditDialogComponent } from '../catalog-object-edit-dialog/catalog-object-edit-dialog.component';
import { ObjectSchemaMap } from '../../models/catalog-objectschema';
import { TeradataPolicy } from '../../models/catalog-teradata-policy';
import { DerivedPolicy } from '../../models/catalog-derived-policy';
import { CatalogDatasetAccessDialogComponent } from '../catalog-dataset-access-policy-dialog/catalog-dataset-access-dialog.component';
import { ObjectAttributeKeyValue } from '../../models/catalog-object-attribute-key-value';
import { CatalogCreateObjectAttributeDialogComponent } from '../../catalog-object-attribute-create/catalog-object-attribute-create-dialog.component';
import {SessionService} from '../../../../core/services/session.service';

@Component({
  selector: 'app-catalog-dataset-action', templateUrl: './catalog-dataset-action.component.html', styleUrls: ['./catalog-dataset-action.component.scss'],
})

export class CatalogDatabaseActionComponent {
  @Input() project: string;
  @Input() storageDataSetId: number;
  @Input() storageDataSetName: string;
  @Input() storageDataSetAliasName: string;
  @Input() storageDataSetDescription: string;
  @Input() storageSystemName: string;
  @Input() createdUser: string;
  @Input() objectId: number;
  @Input() public errorStatus: boolean;
  @Input() isActiveYN: string;
  @Input() isReadCompatible: string;
  @Input() zoneName: string;
  @Input() entityName: string;
  @Input() isAccessControlled: string;
  @Input() teradataPolicies: Array<TeradataPolicy>;
  @Input() derivedPolicies: Array<DerivedPolicy>;
  @Input() admin: boolean;
  @Input() userName: string;
  public inProgress = false;
  public actionMsg: string;
  object: ObjectSchemaMap;
  customAttributes: Array<ObjectAttributeKeyValue>;
  dialogConfig: MatDialogConfig = {width: '1000px', height: '90vh'};
  createDialogConfig: MatDialogConfig = {width: '1000px', height: '50vh'};

  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog, private sessionService: SessionService) {

  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.storageDataSetId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openAccessControlDialog() {
    if (this.teradataPolicies.length > 0) {
      let dialogRef: MatDialogRef<CatalogDatasetAccessDialogComponent>;
      dialogRef = this.dialog.open(CatalogDatasetAccessDialogComponent, this.dialogConfig);
      dialogRef.componentInstance.accessControlList = this.teradataPolicies;
      dialogRef.componentInstance.typeName = 'Teradata';
      dialogRef.componentInstance.datasetName = this.storageDataSetName;
    } else {
      const tempList = [];
      let num = 0;
      for (num = 0; num < this.derivedPolicies.length; num++) {
        const temp = this.derivedPolicies[num];
        const policies = temp.policyItems;
        policies.forEach(policy => {
          tempList.push(policy);
        });
      }
      let dialogRef: MatDialogRef<CatalogDatasetAccessDialogComponent>;
      dialogRef = this.dialog.open(CatalogDatasetAccessDialogComponent, this.dialogConfig);
      dialogRef.componentInstance.accessControlList = tempList;
      dialogRef.componentInstance.typeName = 'Hadoop';
      dialogRef.componentInstance.datasetName = this.storageDataSetName;
    }

  }

  openEditDatasetDialog() {
    let dialogRef: MatDialogRef<CatalogDatasetEditDialogComponent>;
    dialogRef = this.dialog.open(CatalogDatasetEditDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.storageDataSetName = this.storageDataSetName;
    dialogRef.componentInstance.storageDataSetId = this.storageDataSetId;
    dialogRef.componentInstance.projectName = this.project;
    dialogRef.componentInstance.storageDataSetAliasName = this.storageDataSetAliasName;
    dialogRef.componentInstance.storageDataSetDescription = this.storageDataSetDescription;
    dialogRef.componentInstance.createdUser = this.createdUser;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the Dataset with ID -> ' + this.storageDataSetId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Dataset');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update dataset', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update dataset', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  openAddAttributesObjectDialog() {
    this.inProgress = true;
    let dialogRef: MatDialogRef<CatalogCreateObjectAttributeDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateObjectAttributeDialogComponent, this.createDialogConfig);
    dialogRef.componentInstance.objectId = this.objectId;
    this.inProgress = false;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the ObjectAttribute with ID -> ' + result.objectAttributeKeyValueId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Object');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create Object attribute', 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Object');
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create Object attribute', 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Object');
          }
        }
      });
  }


  openEditObjectDialog() {
    this.inProgress = true;
    const objectDetails = this.catalogService.getObjectDetails(this.objectId.toString());
    const customAttributes = this.catalogService.getCustomAttributes(this.objectId.toString());
    forkJoin([objectDetails, customAttributes]).subscribe(results => {
      this.object = results[0];
      this.customAttributes = results[1];
      this.inProgress = false;
      let dialogRef: MatDialogRef<CatalogObjectEditDialogComponent>;
      dialogRef = this.dialog.open(CatalogObjectEditDialogComponent, this.dialogConfig);
      dialogRef.componentInstance.objectName = this.object.objectName;
      dialogRef.componentInstance.objectId = this.objectId;
      dialogRef.componentInstance.containerName = this.object.containerName;
      dialogRef.componentInstance.storageSystemId = this.object.storageSystemId;
      dialogRef.componentInstance.objectSchema = this.object.objectSchema;
      dialogRef.componentInstance.createdUser = this.object.createdUser;
      dialogRef.componentInstance.objectAttributes = this.object.objectAttributes;
      dialogRef.componentInstance.customAttributes = this.customAttributes;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the Object with ID -> ' + result.objectId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Object');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update Object', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update Object', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    });
  }

  deleteDataset() {
    this.actionMsg = 'Deleting dataset';
    this.inProgress = true;
    this.catalogService.deleteDataset(this.storageDataSetId.toString())
      .subscribe(data => {
        this.snackbar.open('Deleted the Dataset with ID -> ' + this.storageDataSetId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Dataset');
      }, error => {
        this.snackbar.open('Dataset ID not found -> ' + this.storageDataSetId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Dataset Deleted';
        this.inProgress = false;
      });
  }
}
