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
import { onValueChanged } from '../../../shared/utils';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { forkJoin } from 'rxjs/observable/forkJoin';
import { TeradataObjectDescription } from '../../../admin/models/catalog-teradata-object-description';
import { DatasetObjectDescription } from '../../../admin/models/catalog-dataset-object-description';
import { SessionService } from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-description-edit-dialog', templateUrl: './catalog-description-edit-dialog.component.html', styleUrls: ['./catalog-description-edit-dialog.component.scss'],
})

export class CatalogDescriptionEditDialogComponent implements OnInit {
  heading = 'Editing Description for ';
  editDescriptionForm: FormGroup;
  maxCharsForDescName = 6000;
  objectDescription: string;
  datasetId: number;
  datasetName: string;
  systemName: string;
  updatedUser: string;
  systemId: number;
  objectId: number;
  objectType: string;
  providerName: string = 'USER';

  formErrors = {
    'modifiedObjectDescription': '',
  };

  validationMessages = {
    'modifiedObjectDescription': {
      'required': 'Description is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
    },
  };

  constructor(public dialogRef: MatDialogRef<CatalogDescriptionEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService, private sessionService: SessionService) {
  }

  ngOnInit() {
    this.editDescriptionForm = this.fb.group({
      'modifiedObjectDescription': ['', [Validators.maxLength(this.maxCharsForDescName)]],
    });

    this.editDescriptionForm.valueChanges.subscribe(data => onValueChanged(this.editDescriptionForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editDescriptionForm, this.formErrors, this.validationMessages);
    this.heading += this.datasetName;

  }

  cancel() {
    this.dialogRef.close();
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editDescriptionForm.value);
    const objectDetails = this.catalogService.getObjectDetails(this.objectId.toString());
    if (this.systemName.indexOf('Teradata') > -1) {
      forkJoin([objectDetails]).subscribe(objectResult => {
        const objectDescriptionDetails = this.catalogService.getDescriptionForTeradata(this.systemId.toString(), objectResult[0].containerName, objectResult[0].objectName, this.providerName);
        forkJoin([objectDescriptionDetails]).subscribe(objectDescriptionResult => {
          // post dataset description if description is not found
          if (objectDescriptionResult[0].schemaDatasetMapId === 0) {
            if (objectResult[0].containerName.toLowerCase().indexOf('view') > -1) {
              this.objectType = 'VIEW';
            } else {
              this.objectType = 'TABLE';
            }
            const postDescriptionObject = new TeradataObjectDescription(this.datasetId, this.systemId, this.objectType, objectResult[0].objectName, objectResult[0].containerName, submitValue.modifiedObjectDescription, this.updatedUser, this.updatedUser, this.providerName);
            this.catalogService.insertTeradataDescription(postDescriptionObject).subscribe(insertedData => {
              this.dialogRef.close({status: 'success', description: insertedData.objectComment});
            }, insertDescError => {
              if (insertDescError.status === 500) {
                this.dialogRef.close({status: 'fail', error: ''});
              } else {
                this.dialogRef.close({status: 'fail', error: insertDescError});
              }
            });
          } else {

            const putDescriptionObject = new TeradataObjectDescription(this.datasetId, this.systemId, objectDescriptionResult[0].objectType, objectResult[0].objectName, objectResult[0].containerName, submitValue.modifiedObjectDescription, objectDescriptionResult[0].createdUser, this.updatedUser, this.providerName);
            putDescriptionObject.schemaDatasetMapId = objectDescriptionResult[0].schemaDatasetMapId;
            this.catalogService.updateTeradataObjectDescription(putDescriptionObject).subscribe(updatedData => {
              this.dialogRef.close({status: 'success', description: updatedData.objectComment});
            }, updateDescError => {
              if (updateDescError.status === 500) {
                this.dialogRef.close({status: 'fail', error: ''});
              } else {
                this.dialogRef.close({status: 'fail', error: updateDescError});
              }
            });
          }
        }, objectDescriptionError => {
        });
      }, objectError => {
      });
    } else {
      forkJoin([objectDetails]).subscribe(objectResult => {
        const objectDescriptionDetails = this.catalogService.getDescriptionForDataset(this.systemId.toString(), objectResult[0].containerName, objectResult[0].objectName, this.providerName);
        forkJoin([objectDescriptionDetails]).subscribe(objectDescriptionResult => {
          // post dataset description if description is not found
          if (objectDescriptionResult[0].bodhiDatasetMapId === 0) {
            const postDescriptionObject = new DatasetObjectDescription(this.datasetId, this.systemId, this.providerName, objectResult[0].objectName, objectResult[0].containerName, submitValue.modifiedObjectDescription, this.updatedUser, this.updatedUser);
            postDescriptionObject.storageSystemName = this.systemName;
            this.catalogService.insertDatasetDescription(postDescriptionObject).subscribe(insertedData => {
              this.dialogRef.close({status: 'success', description: insertedData.objectComment});
            }, insertDescError => {
              if (insertDescError.status === 500) {
                this.dialogRef.close({status: 'fail', error: ''});
              } else {
                this.dialogRef.close({status: 'fail', error: insertDescError});
              }
            });
          } else {
            const putDescriptionObject = new DatasetObjectDescription(this.datasetId, this.systemId, this.providerName, objectResult[0].objectName, objectResult[0].containerName, submitValue.modifiedObjectDescription, objectDescriptionResult[0].createdUser, this.updatedUser);
            putDescriptionObject.storageSystemName = this.systemName;
            putDescriptionObject.bodhiDatasetMapId = objectDescriptionResult[0].bodhiDatasetMapId;
            putDescriptionObject.providerId = objectDescriptionResult[0].providerId;
            this.catalogService.updateDatasetObjectDescription(putDescriptionObject).subscribe(updatedData => {
              this.dialogRef.close({status: 'success', description: updatedData.objectComment});
            }, updateDescError => {
              if (updateDescError.status === 500) {
                this.dialogRef.close({status: 'fail', error: ''});
              } else {
                this.dialogRef.close({status: 'fail', error: updateDescError});
              }
            });
          }
        }, objectDescriptionError => {
        });
      }, objectError => {
      });
    }
  }
}
