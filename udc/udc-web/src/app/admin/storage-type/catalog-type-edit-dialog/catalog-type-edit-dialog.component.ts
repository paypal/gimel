import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Type} from '../../models/catalog-type';
import {StorageSystem} from '../../../udc/catalog/models/catalog-storagesystem';
import {error} from 'util';
import {StorageTypeAttribute} from '../../../udc/catalog/models/catalog-dataset-storagetype-attribute';

@Component({
  selector: 'app-catalog-type-edit-dialog',
  templateUrl: './catalog-type-edit-dialog.component.html',
  styleUrls: ['./catalog-type-edit-dialog.component.scss'],
})

export class CatalogTypeEditDialogComponent implements OnInit {
  heading = 'Edit Type';
  editTypeForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  storageTypeId: number;
  storageTypeName: string;
  tempStorageTypeName: string;
  storageTypeDescription: string;
  tempStorageTypeDescription: string;
  createdUser: string;
  typeAttributes: Array<any>;
  newTypeAttributes: Array<any>;
  selectedSystemLevel: string;
  editing = {};
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'modifiedStorageTypeName': '', 'updatedUser': '', 'modifiedStorageTypeDescription': '',
  };

  validationMessages = {
    'modifiedStorageTypeName': {
      'required': 'Storage Type name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'updatedUser': {
      'required': 'user name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'modifiedStorageTypeDescription': {
      'required': 'Storage Type description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogTypeEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
    this.newTypeAttributes = new Array<any>();
  }

  ngOnInit() {
    this.editTypeForm = this.fb.group({
      'modifiedStorageTypeName': ['', [Validators.maxLength(this.maxCharsForName)]],
      'modifiedStorageTypeDescription': ['', [Validators.maxLength(this.maxCharsForAliasName)]],
      'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
      'attributeKey': [],
      'attributeDesc': [],
      'isStorageSystemLevel': [],
    });

    this.editTypeForm.valueChanges.subscribe(data => onValueChanged(this.editTypeForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editTypeForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateType(submitValue) {
    const data: Type = new Type();
    data.storageTypeId = this.storageTypeId;
    if (submitValue.modifiedStorageTypeName.length > 0) {
      data.storageTypeName = submitValue.modifiedStorageTypeName;
    } else {
      data.storageTypeName = this.storageTypeName;
    }
    if (submitValue.modifiedStorageTypeDescription.length > 0) {
      data.storageTypeDescription = submitValue.modifiedStorageTypeDescription;
    } else {
      data.storageTypeDescription = this.storageTypeDescription;
    }
    data.updatedUser = submitValue.updatedUser;
    data.attributeKeys = this.typeAttributes;
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editTypeForm.value);
    const type: Type = this.populateType(submitValue);
    this.catalogService.getUserByName(type.updatedUser)
      .subscribe(data => {
        this.catalogService.updateType(type)
          .subscribe(result => {
            this.newTypeAttributes.forEach(typeAttribute => {
              const tempTypeAttribute = new StorageTypeAttribute();
              tempTypeAttribute.storageTypeId = this.storageTypeId;
              tempTypeAttribute.createdUser = this.editTypeForm.controls.updatedUser.value;
              tempTypeAttribute.storageDsAttributeKeyDesc = typeAttribute.storageDsAttributeKeyDesc;
              tempTypeAttribute.storageDsAttributeKeyName = typeAttribute.storageDsAttributeKeyName;
              tempTypeAttribute.isStorageSystemLevel = typeAttribute.isStorageSystemLevel;
              this.catalogService.insertTypeAttribute(tempTypeAttribute)
                .subscribe(attributeResult => {
                }, error => {
                  if (error.status === 500) {
                    this.dialogRef.close({status: 'fail', error: ''});
                  } else {
                    this.dialogRef.close({status: 'fail', error: error});
                  }
                });
            });
            this.dialogRef.close({status: 'success', typeId: result.storageTypeId});
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
    this.typeAttributes[row.$$index][cell] = event.target.value;
  }

  addToAttributes() {
    this.newTypeAttributes.push({
      storageDsAttributeKeyName: this.editTypeForm.value.attributeKey,
      storageDsAttributeKeyDesc: this.editTypeForm.value.attributeDesc,
      isStorageSystemLevel: this.editTypeForm.value.isStorageSystemLevel,
    });
    if (!this.editTypeForm.controls.modifiedStorageTypeName.value) {
      this.tempStorageTypeName = this.storageTypeName;
    } else {
      this.tempStorageTypeName = this.editTypeForm.controls.modifiedStorageTypeName.value;
    }
    if (!this.editTypeForm.controls.modifiedStorageTypeDescription.value) {
      this.tempStorageTypeDescription = this.storageTypeDescription;
    } else {
      this.tempStorageTypeDescription = this.editTypeForm.controls.modifiedStorageTypeDescription.value;
    }
    this.editTypeForm.reset({
      modifiedStorageTypeName: this.tempStorageTypeName,
      modifiedStorageTypeDescription: this.tempStorageTypeDescription,
      updatedUser: this.editTypeForm.controls.updatedUser.value,
      attributeKey: '',
      attributeDesc: '',
      isStorageSystemLevel: '',
    });
  }

}
