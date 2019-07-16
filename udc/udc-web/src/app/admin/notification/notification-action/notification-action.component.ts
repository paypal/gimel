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
import { NotificationEditDialogComponent } from '../notification-edit-dialog/notification-edit-dialog.component';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { Notification } from '../../models/catalog-notification';

@Component({
  selector: 'app-catalog-notification-action',
  templateUrl: './notification-action.component.html',
  styleUrls: ['./notification-action.component.scss'],
})

export class NotificationActionComponent {
  @Input() notificationId: number;
  @Input() notificationContent: string;
  @Input() notificationMessagePage: string;
  @Input() notificationPriority: string;
  @Input() notificationType: string;
  @Input() notificationLiveTimeInMin: string;
  @Input() referenceUrl: string;
  @Input() createdUser: string;
  @Input() isActiveYN: string;
  public currentUser: string;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  dialogConfig: MatDialogConfig = {width: '600px'};

  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog) {
    this.currentUser = this.config.userName && this.config.userName != 'udcdev' ? this.config.userName : "drampally";
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.notificationId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openEditNotificationDialog() {
    let dialogRef: MatDialogRef<NotificationEditDialogComponent>;
    dialogRef = this.dialog.open(NotificationEditDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.notificationId = this.notificationId;
    dialogRef.componentInstance.notificationContent = this.notificationContent;
    dialogRef.componentInstance.notificationMessagePage = this.notificationMessagePage;
    dialogRef.componentInstance.notificationPriority = this.notificationPriority;
    dialogRef.componentInstance.notificationType = this.notificationType;
    dialogRef.componentInstance.referenceUrl = this.referenceUrl;
    dialogRef.componentInstance.notificationLiveTimeInMin = this.notificationLiveTimeInMin;
    dialogRef.componentInstance.createdUser = this.createdUser;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the Notification with ID -> ' + this.notificationId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Cluster');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update Notification', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update Notification', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  /*getObjectDetails() {
    this.inProgress = true;
    this.catalogService.getNotificationDetails(this.notification)
      .subscribe(data => {
        this.statusData = data;
        this.inProgress = false;
      }, error => {
        this.statusData = {};
        this.inProgress = false;
      });
  }*/

  private populateNotification() {
    const notification: Notification = new Notification();
    notification.notificationId = this.notificationId;
    notification.notificationContent = this.notificationContent;
    notification.createdUser = this.createdUser;
    notification.updatedUser = this.currentUser;
    notification.notificationPriority = this.notificationPriority;
    notification.notificationType = this.notificationType;
    notification.notificationMessagePage = this.notificationMessagePage;
    notification.referenceUrl = this.referenceUrl;
    notification.notificationLiveTimeInMin = this.notificationLiveTimeInMin;
    return notification;
  }

  deleteNotification() {
    this.actionMsg = 'Deleting Notification';
    this.inProgress = true;
    const Notification: Notification = this.populateNotification();
    Notification.notificationLiveTimeInMin = "0";
    this.catalogService.updateNotification(Notification)
      .subscribe(result => {
        this.snackbar.open('Deactivated the Notification with ID -> ' + this.notificationId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Dataset');
      }, error => {
        if (error.status === 500) {
          this.snackbar.open('Notification ID not found -> ' + this.notificationId, 'Dismiss', this.config.snackBarConfig);
          this.actionMsg = 'Notification Cannot be De-Activated';
          this.inProgress = false;
        } else {
          this.snackbar.open('Notification ID not found -> ' + this.notificationId, 'Dismiss', this.config.snackBarConfig);
          this.actionMsg = 'Notification Cannot be De-Activated';
          this.inProgress = false;
        }
      });
  }

  enableNotification() {
    this.actionMsg = 'Activating Notification';
    this.inProgress = true;
    this.catalogService.enableEntity(this.notificationId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the Notification with ID -> ' + this.notificationId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Notification');
      }, error => {
        this.snackbar.open('Notification ID not found -> ' + this.notificationId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Notification Cannot be Re-Activated';
        this.inProgress = false;
      });
  }
}
