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

import { Component, OnInit } from '@angular/core';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material';
import { CustomValidators, onValueChanged } from '../../../shared/utils';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { Notification } from '../../models/catalog-notification';

@Component({
  selector: 'app-catalog-notification-edit-dialog',
  templateUrl: './notification-edit-dialog.component.html',
  styleUrls: ['./notification-edit-dialog.component.scss'],
})

export class NotificationEditDialogComponent implements OnInit {
  heading = 'Edit Notification';
  editNotificationForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForLiveTimeMin = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  notificationId: number;
  notificationContent: string;
  notificationMessagePage: string;
  notificationPriority: string;
  notificationLiveTimeInMin: string;
  notificationType: string;
  referenceUrl: string;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  
  formErrors = {
    'modifiednotificationContent': '', 'modifiedcreatedUser': '', 'modifiedreferenceUrl': '', 'modifiednotificationMessagePage': '', 'modifiednotificationLiveTimeInMin' : '', 'modifiednotificationPriority': '', 'modifiednotificationType': '', 'updatedUser' : ''
  };

  validationMessages = {
    'modifiednotificationContent': {
      'required': 'Notification name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
    }, 'modifiedcreatedUser': {
      'required': 'username is required.',
      'maxlength': `modifiedcreatedUser cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'updatedUser': {
      'required': 'username is required.',
      'maxlength': `updatedUser cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'modifiedreferenceUrl': {
      'required': 'referenceUrl is required.',
      'maxlength': `modifiedreferenceUrl cannot be more than ${ this.maxCharsForDescName } characters long.`,
    },'modifiednotificationMessagePage': {
      'required': 'Notification MessagePage is required.',
      'maxlength': `modifiednotificationMessagePage cannot be more than ${ this.maxCharsForDescName } characters long.`,
    }, 'modifiednotificationLiveTimeInMin': {
      'required': 'Notification LiveTimeInMin is required.',
      'maxlength': `modifiednotificationLiveTimeInMin cannot be more than ${ this.maxCharsForLiveTimeMin } characters long.`,
    }, 'modifiednotificationPriority': {
      'required': 'Notification LiveTimeInMin is required.',
      'maxlength': `modifiednotificationPriority cannot be more than ${ this.maxCharsForDescName } characters long.`,
    }, 'modifiednotificationType': {
      'required': 'Notification LiveTimeInMin is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
    }
  };

  constructor(public dialogRef: MatDialogRef<NotificationEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editNotificationForm = this.fb.group({
      'modifiednotificationContent': ['', [Validators.maxLength(this.maxCharsForName)]],
      'modifiedreferenceUrl': ['', [Validators.maxLength(this.maxCharsForDescName)]],
      'modifiednotificationMessagePage': ['', [Validators.maxLength(this.maxCharsForDescName)]],
      'modifiednotificationLiveTimeInMin': ['', [Validators.maxLength(this.maxCharsForDescName)]],
      'modifiednotificationPriority': ['', [Validators.maxLength(this.maxCharsForLiveTimeMin)]],
      'modifiednotificationType': ['', [Validators.maxLength(this.maxCharsForAliasName)]],
      'modifiedcreatedUser': ['', [ Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
      'updatedUser': ['', [Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.editNotificationForm.valueChanges.subscribe(data => onValueChanged(this.editNotificationForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editNotificationForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateNotification(submitValue) {
    const data: Notification = new Notification();
    data.notificationId = this.notificationId;
    data.createdUser = this.createdUser;
    data.updatedUser = submitValue.updatedUser;
    if (submitValue.modifiednotificationContent.length > 0) {
      data.notificationContent = submitValue.modifiednotificationContent;
    } else {
      data.notificationContent = this.notificationContent;
    }
    if (submitValue.modifiednotificationMessagePage.length > 0) {
      data.notificationMessagePage = submitValue.modifiednotificationMessagePage;
    } else {
      data.notificationMessagePage = this.notificationMessagePage;
    }
    if (submitValue.modifiednotificationPriority.length > 0) {
      data.notificationPriority = submitValue.modifiednotificationPriority;
    } else {
      data.notificationMessagePage = this.notificationMessagePage;
    }
    if (submitValue.modifiedreferenceUrl.length > 0) {
      data.referenceUrl = submitValue.modifiedreferenceUrl;
    } else {
      data.referenceUrl = this.referenceUrl;
    }
    if (submitValue.modifiednotificationType.length > 0) {
      data.notificationType = submitValue.modifiednotificationType;
    } else {
      data.notificationType = this.notificationType;
    }
    if (submitValue.modifiednotificationLiveTimeInMin.length > 0) {
      data.notificationLiveTimeInMin = submitValue.modifiednotificationLiveTimeInMin;
    } else {
      data.notificationLiveTimeInMin = this.notificationLiveTimeInMin;
    }
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editNotificationForm.value);
    const notification: Notification = this.populateNotification(submitValue);
    this.catalogService.getUserByName(notification.updatedUser)
      .subscribe(data => {
        this.catalogService.updateNotification(notification)
          .subscribe(result => {
            this.dialogRef.close({status: 'success'});
          }, error => {
            if (error.status === 500) {
              this.dialogRef.close({status: 'fail', error: ''});
            } else {
              this.dialogRef.close({status: 'fail', error: error});
            }
          });
      }, error => {
        this.dialogRef.close({status: 'user fail', error: 'Invalid Username'});
      });
  }
}
