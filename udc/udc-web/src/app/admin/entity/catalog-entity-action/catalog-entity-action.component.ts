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
import { Observable } from 'rxjs/Observable';
import { MatSnackBar, MatSnackBarConfig } from '@angular/material';
import {
  MatDialog, MatDialogRef, MatDialogConfig,
} from '@angular/material';

import { ConfigService } from '../../../core/services/config.service';
import { CatalogEntityEditDialogComponent } from '../catalog-entity-edit-dialog/catalog-entity-edit-dialog.component';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-catalog-entity-action',
  templateUrl: './catalog-entity-action.component.html',
  styleUrls: ['./catalog-entity-action.component.scss'],
})

export class CatalogEntityActionComponent {
  @Input() entityId: number;
  @Input() entityName: string;
  @Input() entityDescription: string;
  @Input() createdUser: string;
  @Input() isActiveYN: string;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  dialogConfig: MatDialogConfig = {width: '600px'};

  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.entityId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openEditEntityDialog() {
    let dialogRef: MatDialogRef<CatalogEntityEditDialogComponent>;
    dialogRef = this.dialog.open(CatalogEntityEditDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.entityName = this.entityName;
    dialogRef.componentInstance.entityId = this.entityId;
    dialogRef.componentInstance.entityDescription = this.entityDescription;
    dialogRef.componentInstance.createdUser = this.createdUser;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the Entity with ID -> ' + this.entityId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Cluster');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update Entity', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update entity', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  deleteEntity() {
    this.actionMsg = 'Deleting entity';
    this.inProgress = true;
    this.catalogService.deleteEntity(this.entityId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the Entity with ID -> ' + this.entityId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Dataset');
      }, error => {
        this.snackbar.open('Entity ID not found -> ' + this.entityId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Entity Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableEntity() {
    this.actionMsg = 'Activating Entity';
    this.inProgress = true;
    this.catalogService.enableEntity(this.entityId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the Entity with ID -> ' + this.entityId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Entity');
      }, error => {
        this.snackbar.open('Entity ID not found -> ' + this.entityId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Entity Cannot be Re-Activated';
        this.inProgress = false;
      });
  }
}
