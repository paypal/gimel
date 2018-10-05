import {Component, OnInit} from '@angular/core';
import {forkJoin} from 'rxjs/observable/forkJoin';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../../shared/utils';
import {CatalogService} from '../../services/catalog.service';
import {DatasetWithAttributes} from '../../models/catalog-dataset-attributes';

@Component({
  selector: 'app-catalog-dataset-pending-edit-dialog',
  templateUrl: './catalog-dataset-pending-edit-dialog.component.html',
  styleUrls: ['./catalog-dataset-pending-edit-dialog.component.scss'],
})

export class CatalogDatasetPendingEditDialogComponent implements OnInit {
  heading = 'Edit Dataset';
  editForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  projectName: string;
  storageDataSetId: number;
  storageDataSetName: string;
  storageDataSetAliasName: string;
  storageDataSetDescription: string;
  storageDatasetStatus: string;
  storageSystemName: string;
  objectSchemaMapId: number;
  createdUser: string;
  objectAttributes: Array<any>;
  systemAttributes: Array<any>;
  isAutoRegistered: string;
  storageSystemId: number;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';
  editing = {};

  formErrors = {
    'updatedUser': '',
  };

  validationMessages = {
    'updatedUser': {
      'required': 'user name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogDatasetPendingEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editForm = this.fb.group({
      'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.editForm.valueChanges.subscribe(data => onValueChanged(this.editForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateDataset(submitValue) {
    const data: DatasetWithAttributes = new DatasetWithAttributes();
    data.storageDataSetId = this.storageDataSetId;
    data.storageDataSetName = this.storageDataSetName;
    data.systemAttributes = this.systemAttributes;
    data.objectAttributes = this.objectAttributes;
    data.storageDatasetStatus = this.storageDatasetStatus;
    data.objectSchemaMapId = this.objectSchemaMapId;
    data.storageSystemName = this.storageSystemName;
    data.isAutoRegistered = this.isAutoRegistered;
    data.storageSystemId = this.storageSystemId;
    data.createdUser = this.createdUser;
    data.updatedUser = submitValue.updatedUser;
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editForm.value);
    const schemaMap = this.catalogService.getSchemaDetails(this.objectSchemaMapId.toString());

    forkJoin([schemaMap]).subscribe(results => {
      this.storageSystemId = results[0].storageSystemId;
      const dataset: DatasetWithAttributes = this.populateDataset(submitValue);
      this.catalogService.getUserByName(submitValue.updatedUser)
        .subscribe(data => {
          this.catalogService.updateDatasetWithAttributes(dataset)
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
    });
  }

  updateObjectValue(event, cell, row) {
    this.editing[row.$$index + '-' + cell] = false;
    this.objectAttributes[row.$$index][cell] = event.target.value;
  }

  updateSystemValue(event, cell, row) {
    this.editing[row.$$index + '-' + cell] = false;
    this.systemAttributes[row.$$index][cell] = event.target.value;
  }
}
