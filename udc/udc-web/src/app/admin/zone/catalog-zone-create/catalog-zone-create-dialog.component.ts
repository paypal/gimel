import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Zone} from '../../models/catalog-zone';

@Component({
  selector: 'app-catalog-zone-create-dialog',
  templateUrl: './catalog-zone-create-dialog.component.html',
  styleUrls: ['./catalog-zone-create-dialog.component.scss'],
})

export class CatalogCreateZoneDialogComponent implements OnInit {
  heading = 'Create Zone';
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
    'zoneName': '', 'zoneDescription': '', 'createdUser': '',
  };

  validationMessages = {
    'zoneName': {
      'required': 'Zone name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'zoneDescription': {
      'required': 'Zone description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogCreateZoneDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.createForm = this.fb.group({
      'zoneName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForName), Validators.pattern(this.regex)]],
      'zoneDescription': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
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
    const zone: Zone = this.populateZone(submitValue);
    this.catalogService.getUserByName(zone.createdUser)
      .subscribe(data => {
        this.catalogService.insertZone(zone)
          .subscribe(result => {
            this.dialogRef.close({status: 'success', zoneId: result.zoneId});
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

  private populateZone(submitValue) {
    const zone: Zone = new Zone();
    zone.zoneDescription = submitValue.zoneDescription;
    zone.createdUser = submitValue.createdUser;
    zone.zoneName = submitValue.zoneName;
    return zone;
  }
}
