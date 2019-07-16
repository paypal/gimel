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
import { CatalogSystemViewAttributesDialogComponent } from '../catalog-system-view-attributes-dialog/catalog-system-view-attributes-dialog.component';
import { CatalogSystemEditDialogComponent } from '../catalog-system-edit-dialog/catalog-system-edit-dialog.component';
import { environment } from '../../../../environments/environment';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-system-action', templateUrl: './catalog-system-action.component.html', styleUrls: ['./catalog-system-action.component.scss'],
})

export class CatalogSystemActionComponent {
  @Input() storageSystemId: number;
  @Input() storageSystemName: string;
  @Input() storageSystemDescription: string;
  @Input() createdUser: string;
  @Input() isActiveYN: string;
  @Input() containers: string;
  @Input() adminUserId: number;
  @Input() public errorStatus: boolean;
  @Input() discoverySla: string;
  public inProgress = false;
  public actionMsg: string;
  systemAttributes: Array<any>;

  dialogEditConfig: MatDialogConfig = {width: '600px', height: '90vh'};
  dialogViewConfig: MatDialogConfig = {width: '800px', height: '60vh'};

  @Output() refreshType: EventEmitter<string> = new EventEmitter();
  @Output() refreshEntity: EventEmitter<string> = new EventEmitter();
  @Output() refreshZone: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog, private sessionService: SessionService) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refreshType.emit(this.storageSystemId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  deleteSystem() {
    this.actionMsg = 'Deactivating System';
    this.inProgress = true;
    this.catalogService.deleteSystem(this.storageSystemId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the Type with ID -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Type');
      }, error => {
        this.snackbar.open('System ID not found -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'System Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableSystem() {
    this.actionMsg = 'Activating System';
    this.inProgress = true;
    this.catalogService.enableSystem(this.storageSystemId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the System with ID -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'System');
      }, error => {
        this.snackbar.open('System ID not found -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'System Cannot be Re-Activated';
        this.inProgress = false;
      });
  }

  openAttributesDialog() {
    this.actionMsg = 'Viewing Attributes';
    this.inProgress = true;
    const systemAttributes = this.catalogService.getSystemAttributes(this.storageSystemName);
    forkJoin([systemAttributes]).subscribe(results => {
      this.systemAttributes = results[0];
      this.inProgress = false;
      let dialogRef: MatDialogRef<CatalogSystemViewAttributesDialogComponent>;
      dialogRef = this.dialog.open(CatalogSystemViewAttributesDialogComponent, this.dialogViewConfig);
      dialogRef.componentInstance.storageSystemId = this.storageSystemId;
      dialogRef.componentInstance.systemAttributes = this.systemAttributes;
      dialogRef.componentInstance.storageSystemName = this.storageSystemName;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the System with ID -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Type');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update System', 'Dismiss', this.config.snackBarConfig);
              // this.inProgress = false;
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update System', 'Dismiss', this.config.snackBarConfig);
              // this.inProgress = false;
            }
          }
        });
    }, error => {
    });
  }

  openEditSystemDialog() {
    this.actionMsg = 'Editing Datastore';
    this.inProgress = true;
    const systemAttributes = this.catalogService.getSystemAttributes(this.storageSystemName);
    forkJoin([systemAttributes]).subscribe(results => {
      this.systemAttributes = results[0];
      let dialogRef: MatDialogRef<CatalogSystemEditDialogComponent>;
      dialogRef = this.dialog.open(CatalogSystemEditDialogComponent, this.dialogEditConfig);
      dialogRef.componentInstance.storageSystemName = this.storageSystemName;
      dialogRef.componentInstance.storageSystemId = this.storageSystemId;
      dialogRef.componentInstance.storageSystemDescription = this.storageSystemDescription;
      dialogRef.componentInstance.createdUser = this.createdUser;
      dialogRef.componentInstance.systemAttributes = this.systemAttributes;
      dialogRef.componentInstance.containers = this.containers;
      dialogRef.componentInstance.adminUserId = this.adminUserId;
      dialogRef.componentInstance.discoverySla = this.discoverySla;
      this.inProgress = false;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the System with ID -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Type');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update System', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update System', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    }, error => {
    });

  }
}
