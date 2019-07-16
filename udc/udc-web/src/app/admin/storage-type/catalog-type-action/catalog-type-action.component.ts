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
import { MatSnackBar } from '@angular/material';
import { MatDialog, MatDialogRef, MatDialogConfig } from '@angular/material';
import { forkJoin } from 'rxjs/observable/forkJoin';
import { ConfigService } from '../../../core/services/config.service';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { CatalogTypeEditDialogComponent } from '../catalog-type-edit-dialog/catalog-type-edit-dialog.component';
import { CatalogTypeViewAttributesDialogComponent } from '../catalog-type-view-attributes-dialog/catalog-type-view-attributes-dialog.component';
import { environment } from '../../../../environments/environment';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-type-action', templateUrl: './catalog-type-action.component.html', styleUrls: ['./catalog-type-action.component.scss'],
})

export class CatalogTypeActionComponent {
  @Input() storageTypeId: number;
  @Input() storageTypeName: string;
  @Input() storageTypeDescription: string;
  @Input() createdUser: string;
  @Input() isActiveYN: string;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  typeAttributes: Array<any>;

  dialogEditConfig: MatDialogConfig = {width: '1000px', height: '90vh'};
  dialogViewConfig: MatDialogConfig = {width: '1000px', height: '60vh'};

  @Output() refresh1: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog, private sessionService: SessionService) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh1.emit(this.storageTypeId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openAttributesDialog() {
    this.actionMsg = 'Viewing Datastore Type Attributes';
    this.inProgress = true;
    const typeAttributes = this.catalogService.getTypeAttributes(this.storageTypeId.toString());
    forkJoin([typeAttributes]).subscribe(results => {
      this.inProgress = false;
      this.typeAttributes = results[0];
      let dialogRef: MatDialogRef<CatalogTypeViewAttributesDialogComponent>;
      dialogRef = this.dialog.open(CatalogTypeViewAttributesDialogComponent, this.dialogViewConfig);
      dialogRef.componentInstance.storageTypeId = this.storageTypeId;
      dialogRef.componentInstance.typeAttributes = this.typeAttributes;
      dialogRef.componentInstance.storageTypeName = this.storageTypeName;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the Type with ID -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Type');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update Type', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update Type', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    }, error => {
    });
  }

  openEditTypeDialog() {
    this.actionMsg = 'Editing Datastore Type';
    this.inProgress = true;
    const typeAttributes = this.catalogService.getTypeAttributes(this.storageTypeId.toString());
    forkJoin([typeAttributes]).subscribe(results => {
      this.typeAttributes = results[0];
      this.inProgress = false;
      let dialogRef: MatDialogRef<CatalogTypeEditDialogComponent>;
      dialogRef = this.dialog.open(CatalogTypeEditDialogComponent, this.dialogEditConfig);
      dialogRef.componentInstance.storageTypeName = this.storageTypeName;
      dialogRef.componentInstance.storageTypeId = this.storageTypeId;
      dialogRef.componentInstance.storageTypeDescription = this.storageTypeDescription;
      dialogRef.componentInstance.createdUser = this.createdUser;
      dialogRef.componentInstance.typeAttributes = this.typeAttributes;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the Type with ID -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Type');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update Type', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update Type', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    }, error => {
    });
  }

  deleteType() {
    this.actionMsg = 'Deactivating Type';
    this.inProgress = true;
    this.catalogService.deleteType(this.storageTypeId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the Type with ID -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Type');
      }, error => {
        this.snackbar.open('Type ID not found -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Type Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableType() {
    this.actionMsg = 'Activating Type';
    this.inProgress = true;
    this.catalogService.enableType(this.storageTypeId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the Type with ID -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Type');
      }, error => {
        this.snackbar.open('Type ID not found -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Type Cannot be Re-Activated';
        this.inProgress = false;
      });
  }
}
