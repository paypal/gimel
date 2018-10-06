import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Cluster} from '../../models/catalog-cluster';
import {Category} from '../../models/catalog-category';
import {MdDialog, MdOptionSelectionChange, MdSnackBar} from '@angular/material';
import {Type} from '../../models/catalog-type';

@Component({
  selector: 'app-catalog-type-create-dialog',
  templateUrl: './catalog-type-create-dialog.component.html',
  styleUrls: ['./catalog-type-create-dialog.component.scss'],
})

export class CatalogCreateTypeDialogComponent implements OnInit {
  heading = 'Create Datastore Type';
  createForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForDescName = 100;
  createdUser: string;
  public storageCategories = [];
  public typeAttributes = Array<any>();
  public dbLoading = false;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([A-Za-z0-9]+\-)*[A-Za-z0-9]+)*$';

  formErrors = {
    'storageTypeName': '',
    'storageTypeDescription': '',
    'createdUser': '',
  };

  validationMessages = {
    'storageTypeName': {
      'required': 'Storage name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'storageTypeDescription': {
      'required': 'Storage Type description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogCreateTypeDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.loadStorageCategories();
    this.createForm = this.fb.group({
      'storageTypeName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
      'storageTypeDescription': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
      'createdUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
      'storageCategory': [],
      'attributeKey': [],
      'attributeDesc': [],
      'isStorageSystemLevel': [],

    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  loadStorageCategories() {
    this.dbLoading = true;
    this.catalogService.getStorageCategories()
      .subscribe(data => {
        data.forEach(element => {
          this.storageCategories.push(element);

        });
      }, error => {
        this.storageCategories = [];
        this.dbLoading = false;
      }, () => {
        this.storageCategories = this.storageCategories.sort((a, b): number => {
          return a.storageName > b.storageName ? 1 : -1;
        });
      });
    this.dbLoading = false;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const type: Type = this.populateType(submitValue);
    this.catalogService.getUserByName(type.createdUser)
      .subscribe(data => {
        this.catalogService.insertType(type)
          .subscribe(result => {
            this.dialogRef.close({status: 'success', typeId: result.storageTypeId });
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

  private populateType(submitValue) {
    const type: Type = new Type();
    type.storageTypeDescription = submitValue.storageTypeDescription;
    type.createdUser = submitValue.createdUser;
    type.storageTypeName = submitValue.storageTypeName;
    type.updatedUser = submitValue.createdUser;
    type.storageId = submitValue.storageCategory.storageId;
    type.attributeKeys = this.typeAttributes;
    return type;
  }

  addToAttributes() {
    this.typeAttributes.push({
      storageDsAttributeKeyName: this.createForm.value.attributeKey,
      storageDsAttributeKeyDesc: this.createForm.value.attributeDesc,
      isStorageSystemLevel: this.createForm.value.isStorageSystemLevel,
    });
    this.createForm.reset({
      ...this.createForm.value,
      attributeKey: '',
      attributeDesc: '',
      isStorageSystemLevel: '',
    });
    // this.createForm.value.attributeKey = '';
    // this.createForm.value.attributeDesc = '';
    // this.createForm.value.isStorageSystemLevel = '';
  }
}
