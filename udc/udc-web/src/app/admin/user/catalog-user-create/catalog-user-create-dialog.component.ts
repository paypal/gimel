import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Cluster} from '../../models/catalog-cluster';
import {User} from '../../models/catalog-user';

@Component({
  selector: 'app-catalog-user-create-dialog',
  templateUrl: './catalog-user-create-dialog.component.html',
  styleUrls: ['./catalog-user-create-dialog.component.scss'],
})

export class CatalogCreateUserDialogComponent implements OnInit {
  heading = 'Create User';
  createForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForDescName = 100;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'userName': '', 'userFullName': '',
  };

  validationMessages = {
    'userName': {
      'required': 'User name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'userFullName': {
      'required': 'User Full name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogCreateUserDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.createForm = this.fb.group({
      'userName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
      'userFullName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const user: User = this.populateUser(submitValue);
    this.catalogService.insertUser(user)
      .subscribe(result => {
        this.dialogRef.close({status: 'success', userId: result.userId});
      }, error => {
        if (error.status === 500) {
          this.dialogRef.close({status: 'fail', error: ''});
        } else {
          this.dialogRef.close({status: 'fail', error: error});
        }
      });
  }

  private populateUser(submitValue) {
    const user: User = new User();
    user.userName = submitValue.userName;
    user.userFullName = submitValue.userFullName;
    return user;
  }
}
