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
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { CatalogClassificationEditDialogComponent } from '../catalog-classification-edit-dialog/catalog-classification-edit-dialog.component';

@Component({
  selector: 'app-catalog-classification-action', templateUrl: './catalog-classification-action.component.html', styleUrls: ['./catalog-classification-action.component.scss'],
})

export class CatalogClassificationActionComponent {
  @Input() datasetClassificationId: number;
  @Input() objectName: string;
  @Input() columnName: string;
  @Input() classificationId: number;
  @Input() createdUser: string;
  @Input() classificationComment: string;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  dialogConfig: MatDialogConfig = {width: '1000px', height: '670px'};
  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.datasetClassificationId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openEditClassificationDialog() {
    let dialogRef: MatDialogRef<CatalogClassificationEditDialogComponent>;
    dialogRef = this.dialog.open(CatalogClassificationEditDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.datasetClassificationId = this.datasetClassificationId;
    dialogRef.componentInstance.objectName = this.objectName;
    dialogRef.componentInstance.columnName = this.columnName;
    dialogRef.componentInstance.classificationId = this.classificationId;
    dialogRef.componentInstance.createdUser = this.createdUser;
    dialogRef.componentInstance.classificationComment = this.classificationComment;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the Classification with ID -> ' + result.datasetClassificationId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Classification');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update Classification', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update Classification', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

}
