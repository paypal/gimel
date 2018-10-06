import {Component, OnInit} from '@angular/core';
import { FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Cluster} from '../../models/catalog-cluster';
import {Category} from '../../models/catalog-category';

@Component({
  selector: 'app-catalog-category-edit-dialog',
  templateUrl: './catalog-category-edit-dialog.component.html',
  styleUrls: ['./catalog-category-edit-dialog.component.scss'],
})

export class CatalogCategoryEditDialogComponent implements OnInit {
  heading = 'Edit Cluster';
  editCategoryForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  storageId: number;
  storageName: string;
  storageDescription: string;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'modifiedStorageName': '',
    'updatedUser': '',
    'modifiedStorageDescription': '',
  };

  validationMessages = {
    'modifiedStorageName': {
      'required': 'Storage name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'updatedUser': {
      'required': 'user name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'modifiedStorageDescription': {
      'required': 'Storage description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogCategoryEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editCategoryForm = this.fb.group({
      'modifiedStorageName': ['', [Validators.maxLength(this.maxCharsForName)]],
      'modifiedStorageDescription': ['', [Validators.maxLength(this.maxCharsForAliasName)]],
      'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.editCategoryForm.valueChanges.subscribe(data => onValueChanged(this.editCategoryForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editCategoryForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateCategory(submitValue) {
    const data: Category = new Category();
    data.storageId = this.storageId;
    if (submitValue.modifiedStorageName.length > 0) {
      data.storageName = submitValue.modifiedStorageName;
    }
    if (submitValue.modifiedStorageDescription.length > 0) {
      data.storageDescription = submitValue.modifiedStorageDescription;
    }
    data.updatedUser = submitValue.updatedUser;
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editCategoryForm.value);
    const category: Category = this.populateCategory(submitValue);
    this.catalogService.getUserByName(category.updatedUser)
      .subscribe(data => {
        this.catalogService.updateCategory(category)
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
