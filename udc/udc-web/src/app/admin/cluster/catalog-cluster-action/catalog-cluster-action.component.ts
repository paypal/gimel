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
import { ConfigService } from '../../../core/services/config.service';
import { CatalogClusterEditDialogComponent } from '../catalog-cluster-edit-dialog/catalog-cluster-edit-dialog.component';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { CatalogClusterViewAttributesDialogComponent } from '../catalog-cluster-view-attributes-dialog/catalog-cluster-view-attributes-dialog.component';

@Component({
  selector: 'app-catalog-cluster-action', templateUrl: './catalog-cluster-action.component.html', styleUrls: ['./catalog-cluster-action.component.scss'],
})

export class CatalogClusterActionComponent {
  @Input() clusterId: number;
  @Input() clusterName: string;
  @Input() clusterDescription: string;
  @Input() createdUser: string;
  @Input() isActiveYN: string;
  @Input() attributeKeyValues: Array<any>;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  dialogConfig: MatDialogConfig = {width: '600px'};
  viewDialogConfig: MatDialogConfig = {width: '800px'};
  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.clusterId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openViewAttributesDialog() {
    let dialogRef: MatDialogRef<CatalogClusterViewAttributesDialogComponent>;
    dialogRef = this.dialog.open(CatalogClusterViewAttributesDialogComponent, this.viewDialogConfig);
    dialogRef.componentInstance.clusterId = this.clusterId;
    dialogRef.componentInstance.attributeKeyValues = this.attributeKeyValues;
    dialogRef.componentInstance.clusterName = this.clusterName;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the Cluster with ID -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Type');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update Cluster', 'Dismiss', this.config.snackBarConfig);
            // this.inProgress = false;
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update Cluster', 'Dismiss', this.config.snackBarConfig);
            // this.inProgress = false;
          }
        }
      });

  }

  openEditClusterDialog() {
    let dialogRef: MatDialogRef<CatalogClusterEditDialogComponent>;
    dialogRef = this.dialog.open(CatalogClusterEditDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.clusterName = this.clusterName;
    dialogRef.componentInstance.clusterId = this.clusterId;
    dialogRef.componentInstance.clusterDescription = this.clusterDescription;
    dialogRef.componentInstance.createdUser = this.createdUser;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the Cluster with ID -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Cluster');
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

  deleteCluster() {
    this.actionMsg = 'Deleting Cluster';
    this.inProgress = true;
    this.catalogService.deleteCluster(this.clusterId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the Cluster with ID -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Dataset');
      }, error => {
        this.snackbar.open('Cluster ID not found -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Cluster Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableCluster() {
    this.actionMsg = 'Activating Cluster';
    this.inProgress = true;
    this.catalogService.enableCluster(this.clusterId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the Cluster with ID -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Cluster');
      }, error => {
        this.snackbar.open('Cluster ID not found -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Cluster Cannot be Re-Activated';
        this.inProgress = false;
      });
  }
}
