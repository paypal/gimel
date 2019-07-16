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
import { MatSnackBar, MatSnackBarConfig } from '@angular/material';
import { MatDialog, MatDialogRef, MatDialogConfig } from '@angular/material';

import { CatalogService } from '../../services/catalog.service';
import { ConfigService } from '../../../../core/services';
import { CatalogObjectEditComponent } from '../catalog-object-edit-dialog/catalog-object-edit-dialog.component';
import { ObjectSchemaMap } from '../../models/catalog-objectschema';
import { StorageSystem } from '../../models/catalog-storagesystem';
import { environment } from '../../../../../environments/environment';
import {CatalogCreateObjectAttributeDialogComponent} from '../../catalog-object-attribute-create/catalog-object-attribute-create-dialog.component';
import {SessionService} from '../../../../core/services/session.service';

@Component({
  selector: 'app-catalog-object-action', templateUrl: './catalog-object-action.component.html', styleUrls: ['./catalog-object-action.component.scss'],
})

export class CatalogObjectActionComponent {
  @Input() project: string;
  @Input() objectId: number;
  @Input() objectName: string;
  @Input() containerName: string;
  @Input() storageSystemName: string;
  @Input() createdUser: string;
  @Input() storageSystemId: number;
  @Input() systemMap: Map<number, StorageSystem>;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  object: ObjectSchemaMap;
  dialogConfig: MatDialogConfig = {width: '1000px', height: '90vh'};
  createDialogConfig: MatDialogConfig = {width: '1000px', height: '50vh'};

  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog, private sessionService: SessionService) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.objectId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openAddAttributesObjectDialog() {
    this.inProgress = true;
    let dialogRef: MatDialogRef<CatalogCreateObjectAttributeDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateObjectAttributeDialogComponent, this.createDialogConfig);
    dialogRef.componentInstance.objectName = this.objectName;
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
    forkJoin([objectDetails]).subscribe(results => {
      this.object = results[0];
      this.inProgress = false;
      let dialogRef: MatDialogRef<CatalogObjectEditComponent>;
      dialogRef = this.dialog.open(CatalogObjectEditComponent, this.dialogConfig);
      dialogRef.componentInstance.objectName = this.object.objectName;
      dialogRef.componentInstance.objectId = this.objectId;
      dialogRef.componentInstance.containerName = this.object.containerName;
      dialogRef.componentInstance.storageSystemId = this.object.storageSystemId;
      dialogRef.componentInstance.objectSchema = this.object.objectSchema;
      dialogRef.componentInstance.createdUser = this.object.createdUser;
      dialogRef.componentInstance.objectAttributes = this.object.objectAttributes;
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
    }, error => {
    });
  }
}
