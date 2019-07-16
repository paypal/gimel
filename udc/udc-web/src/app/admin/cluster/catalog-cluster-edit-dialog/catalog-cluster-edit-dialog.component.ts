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
import { Cluster } from '../../models/catalog-cluster';
import { environment } from '../../../../environments/environment';

@Component({
  selector: 'app-catalog-cluster-edit-dialog',
  templateUrl: './catalog-cluster-edit-dialog.component.html',
  styleUrls: ['./catalog-cluster-edit-dialog.component.scss'],
})

export class CatalogClusterEditDialogComponent implements OnInit {
  heading = 'Edit Cluster';
  editClusterForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  clusterId: number;
  clusterName: string;
  clusterDescription: string;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'modifiedClusterName': '',
    'updatedUser': '',
    'modifiedClusterDescription': '',
  };

  validationMessages = {
    'modifiedClusterName': {
      'required': 'Cluster name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'updatedUser': {
      'required': 'user name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'modifiedClusterDescription': {
      'required': 'Cluster description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MatDialogRef<CatalogClusterEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editClusterForm = this.fb.group({
      'modifiedClusterName': ['', [Validators.maxLength(this.maxCharsForName)]],
      'modifiedClusterDescription': ['', [Validators.maxLength(this.maxCharsForDescName)]],
      'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.editClusterForm.valueChanges.subscribe(data => onValueChanged(this.editClusterForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editClusterForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateCluster(submitValue) {
    const data: Cluster = new Cluster();
    data.clusterId = this.clusterId;
    data.createdUser = this.createdUser;
    if (submitValue.modifiedClusterName.length > 0) {
      data.clusterName = submitValue.modifiedClusterName;
    } else {
      data.clusterName = this.clusterName;
    }
    if (submitValue.modifiedClusterDescription.length > 0) {
      data.clusterDescription = submitValue.modifiedClusterDescription;
    } else {
      data.clusterDescription = this.clusterDescription;
    }
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editClusterForm.value);
    const cluster: Cluster = this.populateCluster(submitValue);
    this.catalogService.getUserByName(cluster.createdUser)
      .subscribe(data => {
        this.catalogService.updateCluster(cluster)
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
