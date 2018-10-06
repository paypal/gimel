import {Component, OnInit} from '@angular/core';
import {FormControl, FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../../shared/utils';
import {CatalogService} from '../../services/catalog.service';
import {ObjectSchemaMap} from '../../models/catalog-objectschema';

@Component({
  selector: 'app-catalog-object-edit-dialog',
  templateUrl: './catalog-object-edit-dialog.component.html',
  styleUrls: ['./catalog-object-edit-dialog.component.scss'],
})

export class CatalogObjectEditDialogComponent implements OnInit {
  heading = 'Edit Object';
  editForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  objectId: number;
  objectName: string;
  containerName: string;
  storageSystemId: number;
  createdUser: string;
  objectSchema: Array<any> = new Array<any>();
  objectAttributes: Array<any> = new Array<any>();
  editing = {};
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    // 'modifiedObjectName': '', 'updatedUser': '', 'modifiedContainerName': '',
    'updatedUser': '',
  };

  validationMessages = {
    'updatedUser': {
      'required': 'user name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, // 'modifiedObjectName': {
    //   'required': 'Object name is required.',
    //   'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
    //   'pattern': this.nameHint,
    // }, 'updatedUser': {
    //   'required': 'user name is required.',
    //   'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
    //   'pattern': this.usernameHint,
    // }, 'modifiedContainerName': {
    //   'required': 'container name is required.',
    //   'maxlength': `name cannot be more than ${ this.maxCharsForAliasName } characters long.`,
    //   'pattern': this.usernameHint,
    // },
  };

  constructor(public dialogRef: MdDialogRef<CatalogObjectEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editForm = this.fb.group({
      // 'modifiedObjectName': ['', [Validators.maxLength(this.maxCharsForName)]],
      // 'modifiedContainerName': ['', [Validators.maxLength(this.maxCharsForAliasName)]],
      'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });
    this.editForm.valueChanges.subscribe(data => onValueChanged(this.editForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editForm.value);
    const object: ObjectSchemaMap = new ObjectSchemaMap(this.objectId, this.objectName, this.containerName, this.storageSystemId, submitValue.updatedUser);
    object.objectSchema = this.objectSchema;
    object.objectAttributes = this.objectAttributes;
    this.catalogService.getUserByName(submitValue.updatedUser)
      .subscribe(data => {
        this.catalogService.updateObject(object)
          .subscribe(result => {
            this.dialogRef.close({status: 'success', objectId: this.objectId});
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

  updateValue(event, cell, row) {
    this.editing[row.$$index + '-' + cell] = false;
    this.objectAttributes[row.$$index][cell] = event.target.value;
  }
}
