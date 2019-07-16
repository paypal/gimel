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
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {ExcelService} from '../services/download-excel-service';
import {ConfigService} from '../../../core/services/config.service';
import {NotificationDialogComponent} from '../notification-create/notification-create-dialog.component';
import {NotificationUploadDialogComponent} from '../notification-upload/notification-upload-dialog.component';
import {environment} from '../../../../environments/environment';
import {CloseSideNavAction} from '../../../core/store/sidenav/sidenav.actions';
import {Store} from '@ngrx/store';
import * as fromRoot from '../../../core/store';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-notification-list', templateUrl: './notification-list.component.html',
})
export class NotificationListComponent implements OnInit {
  public loading = false;
  public displayList = [];
  private notificationList = [];
  public systemName = '';
  public entityId = '';
  public inProgress = false;
  public actionMsg: string;
  public admin: boolean;
  dialogConfig: MatDialogConfig = {width: '600px'};
  uploaddialogConfig: MatDialogConfig = {width: '1200px'};
  @ViewChild('catalogNoticiationTable') table: any;

  @Input() project: string;

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private sessionService: SessionService, private store: Store<fromRoot.State>, private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog, private excelService: ExcelService) {
    this.store.dispatch(new CloseSideNavAction());
    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;
  }

  ngOnInit() {
    this.loadNotificationFromCatalog();
  }

  loadNotificationFromCatalog() {
    this.loading = true;
    this.notificationList = [];
    this.catalogService.getNotificationPageList().subscribe(data => {
      data.map(element => {
        this.notificationList.push(element);
      });
    }, error => {
      this.notificationList = [];
    }, () => {
      this.displayList = this.notificationList.sort((a, b): number => {
        return a.entityName > b.entityName ? 1 : -1;
      });
      this.loading = false;
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.notificationList.filter((item, index, array) => {
      return item['entityName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['entityDescription'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  refreshRow(event: string) {
    this.loadNotificationFromCatalog();
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.entityId.toString());
      this.loadNotificationFromCatalog();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  downloadNotificationXLSX(): void {
    let csvArray;
    let csv;
    if (this.notificationList.length <= 0) {
      csv = ['notificationId', 'notificationLiveTimeInMin', 'notificationMessagePage', 'notificationPriority', 'notificationType', 'referenceUrl', 'notificationContent'];
      csvArray = csv.join(',');
    } else {
      const replacer = (key, value) => value === null ? '' : value;
      const header = Object.keys(this.notificationList[0]);
      let csv = this.notificationList.map(row => header.map(fieldName => JSON.stringify(row[fieldName], replacer)).join(','));
      csv.unshift(header.join(','));
      csvArray = csv.join('\r\n');
    }

    this.excelService.exportAsExcelFile(csvArray, 'notfication');
  }

  createNotificationDialog() {
    let dialogRef: MatDialogRef<NotificationDialogComponent>;
    dialogRef = this.dialog.open(NotificationDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the Notification with ID -> ' + result.notificationId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Notifications');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create Notification', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create Notification', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  uploadNotificationDialog() {
    let dialogRef: MatDialogRef<NotificationUploadDialogComponent>;
    dialogRef = this.dialog.open(NotificationUploadDialogComponent, this.uploaddialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the notification with ID -> ' + result.notificationId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Entities');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create notification', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create notification', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }
}
