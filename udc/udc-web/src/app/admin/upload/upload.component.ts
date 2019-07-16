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

import {Component, Input, Output, EventEmitter, ViewChild, OnInit} from '@angular/core';
import {
  MatDialog, MatDialogConfig, MatDialogRef, MatSnackBar
} from '@angular/material';
import {SessionService} from '../../core/services/session.service';
import {Store} from '@ngrx/store';
import {CatalogService} from '../../udc/catalog/services/catalog.service';
import {ConfigService} from '../../core/services/config.service';
import * as fromRoot from '../../core/store';
import {CloseSideNavAction} from '../../core/store/sidenav/sidenav.actions';
import { ClassificationUploadDialogComponent } from './classification-upload/classification-upload-dialog.component';
import { ExcelService } from '../notification/services/download-excel-service';

@Component({
  selector: 'app-udc-admin-upload', templateUrl: './upload.component.html', styleUrls: ['./upload.component.scss'],
})
export class UploadComponent {

  public inProgress = false;
  public actionMsg: string;
  public entityId = '';
  public admin: boolean;
  public classificationAdmin: boolean;
  private sampleClassificationList = [];
  uploaddialogConfig: MatDialogConfig = {width: '1200px'};
  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private sessionService: SessionService, private store: Store<fromRoot.State>, private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog, private excelService: ExcelService) {
    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;
    this.config.getClassificationAdminEmitter().subscribe(data => {
      this.classificationAdmin = data;
    });
    this.classificationAdmin = this.config.classificationAdmin;
    this.store.dispatch(new CloseSideNavAction());
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.entityId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  // uploadOwnershipDialog() {
  //   let dialogRef: MatDialogRef<OwnershipUploadDialogComponent>;
  //   dialogRef = this.dialog.open(OwnershipUploadDialogComponent, this.uploaddialogConfig);
  //   dialogRef.afterClosed()
  //     .subscribe(result => {
  //       if (result) {
  //         if (result.status === 'success') {
  //           this.snackbar.open('Created the Ownership with ID -> ' + result.storageDatasetId, 'Dismiss', this.config.snackBarConfig);
  //           this.finishAction(true, true, 'Entities');
  //         } else if (result.status === 'fail') {
  //           const description = result.error.errorDescription || 'Unknown Error';
  //           this.snackbar.open(description + '.Failed to create Ownership', 'Dismiss', this.config.snackBarConfig);
  //         } else if (result.status === 'user fail') {
  //           const description = 'Invalid Username';
  //           this.snackbar.open(description + '.Failed to create Ownership', 'Dismiss', this.config.snackBarConfig);
  //         }
  //       }
  //     });
  // }

  // uploadNotificationDialog() {
  //   let dialogRef: MatDialogRef<NotificationUploadDialogComponent>;
  //   dialogRef = this.dialog.open(NotificationUploadDialogComponent, this.uploaddialogConfig);
  //   dialogRef.afterClosed()
  //     .subscribe(result => {
  //       if (result) {
  //         if (result.status === 'success') {
  //           this.snackbar.open('Created the notification with ID -> ' + result.notificationId, 'Dismiss', this.config.snackBarConfig);
  //           this.finishAction(true, true, 'Entities');
  //         } else if (result.status === 'fail') {
  //           const description = result.error.errorDescription || 'Unknown Error';
  //           this.snackbar.open(description + '.Failed to create notification', 'Dismiss', this.config.snackBarConfig);
  //         } else if (result.status === 'user fail') {
  //           const description = 'Invalid Username';
  //           this.snackbar.open(description + '.Failed to create notification', 'Dismiss', this.config.snackBarConfig);
  //         }
  //       }
  //     });
  // }

  // uploadDatasetDescriptionDialog() {
  //   let dialogRef: MatDialogRef<DescriptionUploadDialogComponent>;
  //   dialogRef = this.dialog.open(DescriptionUploadDialogComponent, this.uploaddialogConfig);
  //   dialogRef.afterClosed()
  //     .subscribe(result => {
  //       if (result) {
  //         if (result.status === 'success') {
  //           this.snackbar.open('Created the Dataset Description with ID -> ' + result.notificationId, 'Dismiss', this.config.snackBarConfig);
  //           this.finishAction(true, true, 'Entities');
  //         } else if (result.status === 'fail') {
  //           const description = result.error.errorDescription || 'Unknown Error';
  //           this.snackbar.open(description + '.Failed to create Dataset Description', 'Dismiss', this.config.snackBarConfig);
  //         } else if (result.status === 'user fail') {
  //           const description = 'Invalid Username';
  //           this.snackbar.open(description + '.Failed to create Dataset Description', 'Dismiss', this.config.snackBarConfig);
  //         }
  //       }
  //     });
  // }

  uploadClassificationDialog() {
    let dialogRef: MatDialogRef<ClassificationUploadDialogComponent>;
    dialogRef = this.dialog.open(ClassificationUploadDialogComponent, this.uploaddialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Successfully uploaded classifications', 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Entities');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to upload classifications', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to upload classifications', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  downloadClassificationCSV(): void {
    let csvArray;
    const replacer = (key, value) => value === null ? '' : value;
    this.sampleClassificationList = [['objectName, columnName, classificationId, classificationComment'], ['SampleObject, SampleColumn, 3, Please delete this row before uploading']]
    const header = (this.sampleClassificationList[0]);
    const data = (this.sampleClassificationList[1]);
    const csv = this.sampleClassificationList.map(row => header.map(fieldName => JSON.stringify(row[fieldName], replacer)).join(','));
    csv.unshift(data.join(','));
    csv.unshift(header.join(','));
    csvArray = csv.join('\r\n');
    this.excelService.exportAsExcelFile(csvArray, 'classification');
  }

}
