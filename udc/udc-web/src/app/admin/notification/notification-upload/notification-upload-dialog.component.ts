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


import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MatDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {ExcelService} from '../services/download-excel-service';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {ConfigService} from '../../../core/services';
import {Notification} from '../../models/catalog-notification';
type AOA = any[][];

@Component({
  selector: 'app-notification-upload-dialog', templateUrl: './notification-upload-dialog.component.html', styleUrls: ['./notification-upload-dialog.component.scss'],
})

export class NotificationUploadDialogComponent implements OnInit {
  heading = 'Upload Notification';
  createForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  currentUser: string;
  data: any = [];
  editing = {};
  newData: any = [];
  updateData: any = [];
  csvHeader: AOA = [];
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';


  formErrors = {
    'entityName': '', 'entityDescription': '', 'createdUser': '',
  };

  validationMessages = {
    'entityName': {
      'required': 'Notification name is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`, 'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`, 'pattern': this.usernameHint,
    }, 'entityDescription': {
      'required': 'Entity description is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`, 'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MatDialogRef<NotificationUploadDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService, private config: ConfigService, private excelService:ExcelService) {
  }

  ngOnInit() {
    this.createForm = this.fb.group({
      'entityName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForName), Validators.pattern(this.regex)]], 'entityDescription': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]], 'createdUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });
    this.currentUser = this.config.userName && this.config.userName != 'udcdev' ? this.config.userName : 'drampally';
    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  public changeListener(files: FileList){
    if(files && files.length > 0) {
       let file : File = files.item(0); 
         let reader: FileReader = new FileReader();
         reader.readAsText(file);
         reader.onload = (e) => {
            let csv: string = reader.result as string;
            let allTextLines = csv.split(/\r|\n|\r/);
            let headers = allTextLines[0].split(',');
            let lines = [];

            for (let i = 0; i < allTextLines.length; i++) {
              // split content based on comma
              let data = this.excelService.CSVtoArray(allTextLines[i]);
             
              if (data.length === headers.length) {
                let tarr = [];
                for (let j = 0; j < headers.length; j++) {
                  tarr.push(data[j]);
                }
                lines.push(tarr);
              }
            }
            this.data = this.excelService.convertToArrayOfObjects(lines)
         }
      }
  }

  uploadNotification() {
    this.newData = [];
    this.data.map((dataList) => {
      if (!dataList.notificationId || dataList.notificationId == 'null') {
        dataList.createdUser = this.currentUser;
        this.newData.push(dataList);
      } else {
        dataList.updatedUser = this.currentUser;
        this.updateNotification(dataList);
      }
    });

    if (this.newData) {
      this.bulkUpdateNotification();
    }
  }

  bulkUpdateNotification() {
    if (this.newData) {
      this.catalogService.getUserByName(this.currentUser)
        .subscribe(data => {
          this.catalogService.bulkUploadNotification(this.newData)
            .subscribe(result => {
              this.dialogRef.close({status: 'success', notificationId: result.notificationId});
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

  updateNotification(dataList) {
    const notification: Notification = this.populateNotification(dataList);
    this.catalogService.getUserByName(this.currentUser)
      .subscribe(data => {
        this.catalogService.updateNotification(notification)
          .subscribe(result => {
            this.dialogRef.close({status: 'success', notificationId: result.notificationId});
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

  private populateNotification(dataList) {
    const notification: Notification = new Notification();
    notification.notificationContent = dataList.notificationContent;
    notification.notificationId = dataList.notificationId;
    notification.updatedUser = this.currentUser;
    notification.referenceUrl = dataList.referenceUrl;
    notification.notificationMessagePage = dataList.notificationMessagePage;
    notification.notificationLiveTimeInMin = dataList.notificationLiveTimeInMin;
    notification.notificationPriority = dataList.notificationPriority;
    notification.notificationType = dataList.notificationType;
    return notification;
  }

  updateValue(event, cell, rowIndex) {
    this.editing[rowIndex + '-' + cell] = false;
    this.data[rowIndex][cell] = event.target.value;
    this.data = [...this.data];
  }

}
