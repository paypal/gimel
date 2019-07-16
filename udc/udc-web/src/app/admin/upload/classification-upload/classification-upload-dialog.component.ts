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
import { ExcelService } from '../../notification/services/download-excel-service';
import { CustomValidators, onValueChanged } from '../../../shared/utils';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { ConfigService } from '../../../core/services';
import { DatasetComment } from '../../models/catalog-dataset-description-comment';
import { Classification } from '../../models/catalog-classification';

@Component({
  selector: 'app-classification-upload-dialog',
  templateUrl: './classification-upload-dialog.component.html',
  styleUrls: ['./classification-upload-dialog.component.scss'],
})

export class ClassificationUploadDialogComponent implements OnInit {
  heading = 'Upload Classification';
  createForm: FormGroup;
  dataSubmitted = false;
  currentUser: string;
  data: any = [];
  editing = {};
  newData: any = [];
  updateData: any = [];
  public username = '';
  public providerName = 'user';
  private classificationList = [];


  constructor(public dialogRef: MatDialogRef<ClassificationUploadDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService, private config: ConfigService, private excelService: ExcelService) {
    this.config.getUserNameEmitter().subscribe(data => {
      this.username = data;
    });
    this.username = this.config.userName;
  }

  ngOnInit() {
    this.createForm = this.fb.group({});
    this.currentUser = this.config.userName && this.config.userName != 'udcdev' ? this.config.userName : 'drampally';
  }

  cancel() {
    this.dialogRef.close();
  }

  public changeListener(files: FileList) {
    if (files && files.length > 0) {
      const file: File = files.item(0);
      const reader: FileReader = new FileReader();
      reader.readAsText(file);
      reader.onload = (e) => {
        const csv: string = reader.result as string;
        const allTextLines = csv.split(/\r|\n|\r/);
        const headers = allTextLines[0].split(',');
        const lines = [];

        for (let i = 0; i < allTextLines.length; i++) {
          // split content based on comma
          const data = this.excelService.CSVtoArray(allTextLines[i]);
          if (data.length === headers.length) {
            const tarr = [];
            for (let j = 0; j < headers.length; j++) {
              tarr.push(data[j]);
            }
            lines.push(tarr);
          }
        }
        this.data = this.excelService.convertToArrayOfObjects(lines);
      };
    }
  }

  uploadClassification() {
    this.dataSubmitted = true;
    this.populateClassification();
    this.catalogService.insertBulkClassification(this.classificationList)
      .subscribe(result => {
        this.dialogRef.close({status: 'success'});
      }, error => {
        if (error.status === 500) {
          this.dialogRef.close({status: 'fail', error: ''});
        } else {
          this.dialogRef.close({status: 'fail', error: error});
        }
      });
  }

  populateClassification() {
    const selectedStorageDatasetIds = [];
    this.data.forEach(row => {
      const classification: Classification = new Classification(0, row.classificationId, row.objectName, row.columnName, this.providerName, row.classificationComment, this.username, this.username, selectedStorageDatasetIds);
      this.classificationList.push(classification);
    });
  }

  updateValue(event, cell, rowIndex) {
    this.editing[rowIndex + '-' + cell] = false;
    this.data[rowIndex][cell] = event.target.value;
    this.data = [...this.data];
  }

}
