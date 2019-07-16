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
import {ExcelService} from '../../notification/services/download-excel-service';
import { CustomValidators, onValueChanged } from '../../../shared/utils';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { ConfigService } from '../../../core/services';
import { Ownership } from '../../models/catalog-ownership';

@Component({
  selector: 'app-ownership-upload-dialog',
  templateUrl: './ownership-upload-dialog.component.html',
  styleUrls: ['./ownership-upload-dialog.component.scss'],
})

export class OwnershipUploadDialogComponent implements OnInit {
  heading = 'Upload Ownership';
  createForm: FormGroup;
  currentUser: string;
  data:any = [];
  editing = {};
  newData: any =[];
  updateData:any = [];

  constructor(public dialogRef: MatDialogRef<OwnershipUploadDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService, private config: ConfigService, private excelService:ExcelService) {
  }

  ngOnInit() {
    this.createForm = this.fb.group({
    });
    this.currentUser = this.config.userName && this.config.userName != 'udcdev' ? this.config.userName : "drampally";
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

  uploadOwnership(){
    this.newData = [];
    this.data.map((dataList)  => {
      this.updateOwnership(dataList);
    })

    if(this.newData){
      this.bulkUpdateNotification();
    }
  }

  bulkUpdateNotification(){
    if(this.newData){
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

  updateOwnership(dataList) {
    const ownership: Ownership = this.populateOwnership(dataList);
    this.catalogService.getUserByName(this.currentUser)
      .subscribe(data => {
        this.catalogService.updateOwnership(ownership)
          .subscribe(result => {
            if(result.status === 200){
              this.dialogRef.close({status: 'success', storageDatasetId: result.storageDatasetId});
            }else{
              this.dialogRef.close({status: 'fail', error: ''});
            }
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

  private populateOwnership(dataList) {
    const ownership: Ownership = new Ownership();
    ownership.containerName = dataList.containerName;
    ownership.createdUser = dataList.createdUser;
    ownership.emailIlist = dataList.emailIlist;
    ownership.objectName = dataList.objectName;
    ownership.otherOwners = dataList.otherOwners;
    ownership.ownerEmail = dataList.ownerEmail;
    ownership.ownerName = dataList.ownerName;
    ownership.ownershipComment = dataList.ownershipComment;
    ownership.storageSystemName = dataList.storageSystemName;
    ownership.providerName = dataList.providerName;
    ownership.storageDatasetId = dataList.storageDatasetId;
    ownership.storageSystemId = dataList.storageSystemId;
    return ownership;
  }

  updateValue(event, cell, rowIndex) {
    this.editing[rowIndex + '-' + cell] = false;
    this.data[rowIndex][cell] = event.target.value;
    this.data = [...this.data];
  }
}
