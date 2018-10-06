import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Cluster} from '../../models/catalog-cluster';
import {User} from '../../models/catalog-user';

@Component({
  selector: 'app-catalog-user-edit-dialog',
  templateUrl: './catalog-user-edit-dialog.component.html',
  styleUrls: ['./catalog-user-edit-dialog.component.scss'],
})

export class CatalogUserEditDialogComponent implements OnInit {
  heading = 'Edit User';
  editUserForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForUserFullName = 100;
  userId: number;
  userName: string;
  userFullName: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'modifiedUserName': '', 'modifiedUserFullName': '',
  };

  validationMessages = {
    'modifiedUserName': {
      'required': 'User name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.nameHint,
    }, 'modifiedUserFullName': {
      'required': 'User Full Name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserFullName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogUserEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editUserForm = this.fb.group({
      'modifiedUserName': ['', [Validators.maxLength(this.maxCharsForUserName)]],
      'modifiedUserFullName': ['', [Validators.maxLength(this.maxCharsForUserFullName)]],
    });

    this.editUserForm.valueChanges.subscribe(data => onValueChanged(this.editUserForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editUserForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateUser(submitValue) {
    const data: User = new User();
    data.userId = this.userId;
    if (submitValue.modifiedUserName.length > 0) {
      data.userName = submitValue.modifiedUserName;
    }
    if (submitValue.modifiedUserFullName.length > 0) {
      data.userFullName = submitValue.modifiedUserFullName;
    }
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editUserForm.value);
    const user: User = this.populateUser(submitValue);

    this.catalogService.updateUser(user)
      .subscribe(result => {
        this.dialogRef.close({status: 'success'});
      }, error => {
        if (error.status === 500) {
          this.dialogRef.close({status: 'fail', error: ''});
        } else {
          this.dialogRef.close({status: 'fail', error: error});
        }
      });

  }
}
