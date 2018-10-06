import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Zone} from '../../models/catalog-zone';

@Component({
  selector: 'app-catalog-zone-edit-dialog',
  templateUrl: './catalog-zone-edit-dialog.component.html',
  styleUrls: ['./catalog-zone-edit-dialog.component.scss'],
})

export class CatalogZoneEditDialogComponent implements OnInit {
  heading = 'Edit Zone';
  editZoneForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  zoneId: number;
  zoneName: string;
  zoneDescription: string;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'modifiedZoneName': '',
    'updatedUser': '',
    'modifiedZoneDescription': '',
  };

  validationMessages = {
    'modifiedZoneName': {
      'required': 'Zone name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'updatedUser': {
      'required': 'user name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'modifiedZoneDescription': {
      'required': 'Zone description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MdDialogRef<CatalogZoneEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editZoneForm = this.fb.group({
      'modifiedZoneName': ['', [Validators.maxLength(this.maxCharsForName)]],
      'modifiedZoneDescription': ['', [Validators.maxLength(this.maxCharsForDescName)]],
      'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.editZoneForm.valueChanges.subscribe(data => onValueChanged(this.editZoneForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editZoneForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateZone(submitValue) {
    const data: Zone = new Zone();
    data.zoneId = this.zoneId;
    data.createdUser = this.createdUser;
    if (submitValue.modifiedZoneName.length > 0) {
      data.zoneName = submitValue.modifiedZoneName;
    } else {
      data.zoneName = this.zoneName;
    }
    if (submitValue.modifiedZoneDescription.length > 0) {
      data.zoneDescription = submitValue.modifiedZoneDescription;
    } else {
      data.zoneDescription = this.zoneDescription;
    }
    data.updatedUser = submitValue.updatedUser;
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editZoneForm.value);
    const zone: Zone = this.populateZone(submitValue);
    this.catalogService.getUserByName(zone.updatedUser)
      .subscribe(data => {
        this.catalogService.updateZone(zone)
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
