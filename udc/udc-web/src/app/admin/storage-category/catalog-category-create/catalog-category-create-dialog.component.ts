import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Cluster} from '../../models/catalog-cluster';
import {Category} from '../../models/catalog-category';

@Component({
  selector: 'app-catalog-category-create-dialog',
  templateUrl: './catalog-category-create-dialog.component.html',
  styleUrls: ['./catalog-category-create-dialog.component.scss'],
})

export class CatalogCreateCategoryDialogComponent implements OnInit {
  heading = 'Create Datastore Category';
  createForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([A-Za-z0-9]+\-)*[A-Za-z0-9]+)*$';

  formErrors = {
    'storageName': '',
    'storageDescription': '',
    'createdUser': '',
  };

  validationMessages = {
    'storageName': {
      'required': 'Storage name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'storageDescription': {
      'required': 'Storage description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogCreateCategoryDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.createForm = this.fb.group({
      'storageName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
      'storageDescription': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
      'createdUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const category: Category = this.populateCategory(submitValue);
    this.catalogService.getUserByName(category.createdUser)
      .subscribe(data => {
        this.catalogService.insertCategory(category)
          .subscribe(result => {
            this.dialogRef.close({status: 'success', categoryId: result.storageId });
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

  private populateCategory(submitValue) {
    const category: Category = new Category();
    category.storageDescription = submitValue.storageDescription;
    category.createdUser = submitValue.createdUser;
    category.storageName = submitValue.storageName;
    category.updatedUser = submitValue.createdUser;
    return category;
  }
}
