import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Cluster} from '../../models/catalog-cluster';

@Component({
  selector: 'app-catalog-cluster-create-dialog',
  templateUrl: './catalog-cluster-create-dialog.component.html',
  styleUrls: ['./catalog-cluster-create-dialog.component.scss'],
})

export class CatalogCreateClusterDialogComponent implements OnInit {
  heading = 'Create Cluster';
  createForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'clusterName': '', 'clusterDescription': '', 'createdUser': '', 'livyEndPoint': '', 'livyPort': '',
  };

  validationMessages = {
    'clusterName': {
      'required': 'Cluster name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'clusterDescription': {
      'required': 'Cluster description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    }, 'livyEndPoint': {
      'required': 'Livy End point is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    }, 'livyPort': {
      'required': 'Livy Port is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }
  };

  constructor(public dialogRef: MdDialogRef<CatalogCreateClusterDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.createForm = this.fb.group({
      'clusterName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForName), Validators.pattern(this.regex)]],
      'clusterDescription': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
      'createdUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
      'livyEndPoint': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
      'livyPort': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForName), Validators.pattern(this.regex)]],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const cluster: Cluster = this.populateCluster(submitValue);
    this.catalogService.getUserByName(cluster.createdUser)
      .subscribe(data => {
        this.catalogService.insertCluster(cluster)
          .subscribe(result => {
            this.dialogRef.close({status: 'success', clusterId: result.clusterId});
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

  private populateCluster(submitValue) {
    const cluster: Cluster = new Cluster();
    cluster.clusterDescription = submitValue.clusterDescription;
    cluster.createdUser = submitValue.createdUser;
    cluster.clusterName = submitValue.clusterName;
    cluster.livyEndPoint = submitValue.livyEndPoint;
    cluster.livyPort = submitValue.livyPort;
    return cluster;
  }
}
