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
import {onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {forkJoin} from 'rxjs/observable/forkJoin';
import {SessionService} from '../../../core/services/session.service';
import {Ownership} from '../../../admin/models/catalog-ownership';
import {COMMA, ENTER} from '@angular/cdk/keycodes';
import {MatChipInputEvent} from '@angular/material';
import {ThemePalette} from '@angular/material';

export interface ChipColor {
  name: string;
  color: ThemePalette;
}

@Component({
  selector: 'app-ownership-claim-dialog', templateUrl: './catalog-ownership-claim-dialog.component.html', styleUrls: ['./catalog-ownership-claim-dialog.component.scss'],
})

export class CatalogOwnershipClaimDialogComponent implements OnInit {
  heading = 'Claim Ownership (All fields Optional)';
  createForm: FormGroup;
  maxCharsForName = 50;
  maxCharsForDescName = 200;
  ownerName: string;
  ownerEmail: string;
  storageSystemId: number;
  storageSystemName: string;
  objectId: number;
  datasetId: number;
  loading: boolean;
  otherOwners: Array<string>;
  existingEmailIlist: Array<string>;
  existingComment: string;
  providerName = 'USER';
  public availableColor: ChipColor = {name: 'Primary', color: 'primary'};
  visible = true;
  selectable = true;
  removable = true;
  addOnBlur = true;
  readonly separatorKeysCodes: number[] = [ENTER, COMMA];
  formErrors = {
    'emailIlist': '', 'ownershipJustification': '',
  };

  validationMessages = {};

  constructor(private sessionService: SessionService, public dialogRef: MatDialogRef<CatalogOwnershipClaimDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
    this.loading = false;
  }

  ngOnInit() {
    this.createForm = this.fb.group({
      'emailIlist': [], 'miscOwners': [], 'ownershipJustification': [],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }


  addOwner(event: MatChipInputEvent): void {
    const input = event.input;
    const value = event.value;

    // Add our fruit
    if ((value || '').trim()) {
      this.otherOwners.push(value.trim());
    }

    // Reset the input value
    if (input) {
      input.value = '';
    }
  }

  removeOwner(owner: string): void {
    const index = this.otherOwners.indexOf(owner);

    if (index >= 0) {
      this.otherOwners.splice(index, 1);
    }
  }


  addEmail(event: MatChipInputEvent): void {
    const input = event.input;
    const value = event.value;

    // Add our fruit
    if ((value || '').trim()) {
      this.existingEmailIlist.push(value.trim());
    }

    // Reset the input value
    if (input) {
      input.value = '';
    }
  }

  removeEmail(email: string): void {
    const index = this.existingEmailIlist.indexOf(email);

    if (index >= 0) {
      this.existingEmailIlist.splice(index, 1);
    }
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    this.loading = true;
    const objectDetails = this.catalogService.getObjectDetails(this.objectId.toString());
    forkJoin([objectDetails]).subscribe(objectResult => {
      const datasetOwner = new Ownership();
      datasetOwner.storageDatasetId = this.datasetId;
      datasetOwner.providerName = this.providerName;
      datasetOwner.storageSystemId = this.storageSystemId;
      datasetOwner.storageSystemName = this.storageSystemName;
      datasetOwner.emailIlist = submitValue.emailIlist == null ? this.existingEmailIlist.join(',') : submitValue.emailIlist.join(',');
      datasetOwner.ownershipComment = submitValue.ownershipJustification == null ? this.existingComment : submitValue.ownershipJustification;
      datasetOwner.objectName = objectResult[0].objectName;
      datasetOwner.containerName = objectResult[0].containerName;
      datasetOwner.ownerName = this.ownerName;
      datasetOwner.ownerEmail = this.ownerEmail;
      datasetOwner.createdUser = this.ownerName;
      datasetOwner.otherOwners = submitValue.miscOwners == null ? this.otherOwners.join(',') : submitValue.miscOwners.join(',');
      this.catalogService.insertOwnership(datasetOwner)
        .subscribe(result => {
          this.loading = false;
          const datasetOwnershipMapIds = result.map(entry => entry.datasetOwnershipMapId).join(',');
          this.dialogRef.close({status: 'success', datasetOwnershipMapId: datasetOwnershipMapIds, response: result});
        }, error => {
          if (error.status === 500) {
            this.dialogRef.close({status: 'fail', error: ''});
          } else {
            this.dialogRef.close({status: 'fail', error: error});
          }
        });
    }, objectError => {
      this.loading = false;
    });

  }
}
