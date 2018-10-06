import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {MdDialog, MdSnackBar} from '@angular/material';
import {ConfigService} from '../../../core/services/config.service';
import {ObjectSchemaMap} from '../models/catalog-objectschema';
import {Dataset} from '../models/catalog-dataset';
import {StorageSystem} from '../models/catalog-storagesystem';

@Component({
  selector: 'app-catalog-object-create-dialog', templateUrl: './catalog-object-create-dialog.component.html', styleUrls: ['./catalog-object-create-dialog.component.scss'],
})

export class CatalogCreateObjectDialogComponent implements OnInit {
  heading = 'Create Object';
  createForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForDescName = 1000;
  createdUser: string;
  public storageSystems = [];
  public clusterList = [];
  public typeAttributes = Array<any>();
  public dbLoading = false;
  rows = [];
  systemId: number;
  editing = {};
  public options = Array<string>();
  public wantToRegister: boolean;
  public user: any;
  userId: number;
  userName: string;
  storageSystem: StorageSystem;
  systemName: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([A-Za-z0-9]+\_)*[A-Za-z0-9]+)*$';
  formErrors = {
    'objectName': '', 'containerName': '', 'createdUser': '',
  };

  validationMessages = {
    'objectName': {
      'required': 'Object Name is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`, 'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`, 'pattern': this.usernameHint,
    }, 'containerName': {
      'required': 'Container Name is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`, 'pattern': this.nameHint,
    },
  };

  constructor(private config: ConfigService, public dialogRef: MdDialogRef<CatalogCreateObjectDialogComponent>, private snackbar: MdSnackBar, private dialog: MdDialog, private fb: FormBuilder, private catalogService: CatalogService) {
    this.options.push('Yes');
    this.options.push('No');
  }

  ngOnInit() {
    this.loadStorageSystems();
    this.loadClusterList();
    this.createForm = this.fb.group({
      'objectName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForName), Validators.pattern(this.regex)]], 'containerName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]], 'createdUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]], 'storageSystem': [], 'wantToRegister': [],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  loadClusterList() {
    this.catalogService.getClusterList().subscribe(data => {
      data.forEach(element => {
        this.clusterList.push(element.clusterId);
      });
    }, error => {
      this.clusterList = [];
    });
  }

  loadStorageSystems() {
    this.dbLoading = true;
    this.catalogService.getStorageSystems()
      .subscribe(data => {
        data.forEach(element => {
          this.storageSystems.push(element);
        });
      }, error => {
        this.storageSystems = [];
        this.dbLoading = false;
      }, () => {
        this.storageSystems = this.storageSystems.sort((a, b): number => {
          return a.storageSystemName > b.storageSystemName ? 1 : -1;
        });
        this.dbLoading = false;
      });
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const objectSchema: ObjectSchemaMap = this.populateObject(submitValue);
    this.catalogService.getUserByName(objectSchema.createdUser)
      .subscribe(userOutput => {
        this.catalogService.insertObject(objectSchema)
          .subscribe(objectOutput => {
            if (this.wantToRegister) {
              this.user = userOutput;
              this.userId = this.user.userId;
              this.userName = this.user.userName;
              this.systemName = this.storageSystems.filter(storageSystem => storageSystem.storageSystemId === objectOutput.storageSystemId)[0].storageSystemName;
              const dataset = this.populateDataset(objectOutput);
              this.catalogService.insertDataset(dataset).subscribe(datasetOutput => {
                this.dialogRef.close({
                  status: 'success', objectId: objectOutput.objectId, wantToRegister: this.wantToRegister, datasetId: datasetOutput,
                });
              }, error => {
                if (error.status === 500) {
                  this.dialogRef.close({status: 'fail', error: ''});
                } else {
                  this.dialogRef.close({status: 'fail', error: error});
                }
              });
            } else {
              this.dialogRef.close({
                status: 'success', objectId: objectOutput.objectId, wantToRegister: this.wantToRegister,
              });
            }
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

  populateDataset(result: any) {
    const datasetName = this.systemName + '.' + result.containerName + '.' + result.objectName;
    const dataset: Dataset = new Dataset(datasetName, 0, this.systemName, true, result.objectId, datasetName, '', this.userName, this.userName, 'Y', '', 'N', [], [], 'Y');
    dataset.createdUser = this.userName;
    dataset.updatedUser = this.userName;
    dataset.storageDataSetName = datasetName;
    dataset.storageDataSetDescription = datasetName + ' created by ' + this.userName;
    dataset.storageSystemId = this.storageSystem.storageSystemId;
    dataset.storageContainerName = result.containerName;
    dataset.clusters = this.clusterList;
    dataset.objectName = result.objectName;
    dataset.isAutoRegistered = 'N';
    dataset.userId = this.userId;
    return dataset;
  }

  private populateObject(submitValue) {
    const objectSchema: ObjectSchemaMap = new ObjectSchemaMap(0, submitValue.objectName, submitValue.containerName, this.systemId, submitValue.createdUser);
    const objectAttributes = [];
    objectSchema.clusters = this.clusterList;
    objectSchema.objectSchema = [];
    objectSchema.isSelfDiscovered = 'Y';
    this.typeAttributes.forEach(attr => {
      const systemAttr = {
        objectAttributeValue: attr.storageTypeAttributeValue, storageDsAttributeKeyId: attr.storageDsAttributeKeyId,
      };
      objectAttributes.push(systemAttr);
    });
    objectSchema.objectAttributes = objectAttributes;
    return objectSchema;
  }

  onStorageTypeChange() {
    this.storageSystem = Object.assign({}, this.createForm.value).storageSystem;
    this.systemId = this.storageSystem.storageSystemId;
    this.typeAttributes = [];
    this.catalogService.getTypeAttributesAtSystemLevel(this.storageSystem.storageTypeId.toString(), 'N')
      .subscribe(data => {
        data.forEach(element => {
          this.typeAttributes.push(element);
        });
      }, error => {
        this.typeAttributes = [];
        this.snackbar.open('Invalid Storage Type', 'Dismiss', this.config.snackBarConfig);
      });
  }

  captureRegistrationRequest() {
    const registrationStatus = Object.assign({}, this.createForm.value).wantToRegister;
    switch (registrationStatus) {
      case 'Yes': {
        this.wantToRegister = true;
        break;
      }
      case 'No': {
        this.wantToRegister = false;
        break;
      }
      default: {
        this.wantToRegister = true;
        break;
      }
    }
  }

  updateValue(event, cell, row) {
    this.editing[row.$$index + '-' + cell] = false;
    this.typeAttributes[row.$$index][cell] = event.target.value;
  }
}

