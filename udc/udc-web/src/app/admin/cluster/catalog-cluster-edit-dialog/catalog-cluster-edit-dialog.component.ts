import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Cluster} from '../../models/catalog-cluster';

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
  livyPort: number;
  livyEndPoint: string;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'modifiedClusterName': '',
    'updatedUser': '',
    'modifiedClusterDescription': '',
    'modifiedLivyPort': '',
    'modifiedLivyEndPoint': '',
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
    }, 'modifiedLivyEndPoint': {
      'required': 'Livy Endpoint is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    }, 'modifiedLivyPort': {
      'required': 'Livy port is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogClusterEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editClusterForm = this.fb.group({
      'modifiedClusterName': ['', [Validators.maxLength(this.maxCharsForName)]],
      'modifiedClusterDescription': ['', [Validators.maxLength(this.maxCharsForDescName)]],
      'modifiedLivyEndPoint': ['', [Validators.maxLength(this.maxCharsForDescName)]],
      'modifiedLivyPort': ['', [Validators.maxLength(this.maxCharsForName)]],
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
    if (submitValue.modifiedLivyPort) {
      data.livyPort = submitValue.modifiedLivyPort;
    } else {
      data.livyPort = this.livyPort;
    }
    if (submitValue.modifiedLivyEndPoint.length > 0) {
      data.livyEndPoint = submitValue.modifiedLivyEndPoint;
    } else {
      data.livyEndPoint = this.livyEndPoint;
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
