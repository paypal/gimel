import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MdDialog, MdOptionSelectionChange, MdSnackBar} from '@angular/material';
import {ActivatedRoute, Router} from '@angular/router';
import 'rxjs/add/operator/startWith';
import 'rxjs/add/operator/map';
import {ConfigService} from '../../../core/services';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../services/catalog.service';
import {Dataset} from '../models/catalog-dataset';
import {StorageTypeAttribute} from '../models/catalog-dataset-storagetype-attribute';
import {DatasetAttributeValue} from '../models/catalog-dataset-attribute-value';

@Component({
  selector: 'app-register-dataset', templateUrl: './catalog-register-dataset.component.html', styleUrls: ['./catalog-register-dataset.component.scss'],
})
export class CatalogRegisterDatasetComponent implements OnInit {
  public datasetForm: FormGroup;
  public storageCategories = [];
  public storageTypes = [];
  public storageSystems = [];
  public allClusterIds = new Array<number>();
  public allClusterNames = new Array<string>();
  public storageTypeAttributes = new Array() as Array<StorageTypeAttribute>;
  public dataSetAttributes = new Array() as Array<DatasetAttributeValue>;
  public selectedStorageType: string;
  public selectedFileFormat: string;
  public dbLoading = false;
  public submitting = false;
  public project: string;
  public user: any;
  public username: string;

  public readonly datasetHint = 'Valid characters are a-z, A-Z. Names should not start with special chars.';
  public readonly hdfsPathHint = 'Valid HDFS Path.';
  public readonly schemaPathHint = 'Valid Schema Path.';
  public readonly fieldTerminatorHint = 'Valid Field Terminator';
  public readonly rowTerminatorHint = 'Valid Row Terminator';

  containers: Array<any>;
  filteredContainers: Array<any>;
  objects: Array<any>;
  filteredObjects: Array<any>;
  objectName: string;

  public formErrors = {
    'clusterName': '', 'description': '', 'fileFormat': '', 'dataset': '', 'hdfspath': '', 'schemaPath': '', 'rowTerminator': '', 'fieldTerminator': '',
  };
  private validationMessages = {
    'description': {
      'required': 'Description for this dataset registration is required.',
    }, 'fileFormat': {
      'required': 'FileFormat is required.',
    }, 'dataset': {
      'required': 'Dataset name is required',
    }, 'hdfspath': {
      'required': 'HDFS Path is required',
    }, 'schemaPath': {
      'required': 'Schema Path is required',
    }, 'fieldTerminator': {
      'required': 'Field Terminator is required',
    }, 'rowTerminator': {
      'required': 'Row Terminator is required',
    },
  };

  constructor(private config: ConfigService, private catalogService: CatalogService, private formBuilder: FormBuilder, private activatedRoute: ActivatedRoute, private router: Router, private snackbar: MdSnackBar, private dialog: MdDialog) {
    this.username = 'udcdev';
  }

  ngOnInit() {
    this.loadStorageCategories();
    this.loadStorageTypes();
    this.loadStorageSystems();
    this.catalogService.getClusterList().subscribe(data => {
      data.forEach(cluster => {
        this.allClusterIds.push(cluster.clusterId);
        this.allClusterNames.push(cluster.clusterName);
      });
    });
    this.buildForm();
  }

  buildForm() {
    this.datasetForm = this.formBuilder.group({
      'storageCategory': ['', [Validators.required]], 'storageType': ['', [Validators.required]], 'storageSystem': ['', [Validators.required]], 'container': ['', [Validators.required]], 'object': ['', [Validators.required]], 'description': ['', [CustomValidators.required]], 'requesterID': [], 'dataset': ['', [CustomValidators.required]], // 'inputVal': ['', [CustomValidators.required]],
    });
    this.datasetForm.valueChanges.subscribe(data => onValueChanged(this.datasetForm, this.formErrors, this.validationMessages));
  }

  onFileFormatChange() {
    this.selectedFileFormat = this.datasetForm.controls.fileFormat.value;
  }

  onStorageCategoryChange() {
    const storageCategory = this.datasetForm.controls.storageCategory.value;
    if (storageCategory) {
      this.loadStorageTypesForCategory(storageCategory.storageId);
    }
  }

  onStorageTypeChange() {
    const storageType = this.datasetForm.controls.storageType.value;
    if (storageType) {
      this.selectedStorageType = storageType.storageTypeName;
      this.loadStorageSystemsForType(storageType.storageTypeId);
      this.loadStorageTypeAttributesForType(storageType.storageTypeId, 'N');

    }
  }

  onStorageSystemChange() {
    const storageSystem = this.datasetForm.controls.storageSystem.value;
    if (storageSystem) {
      this.loadContainersForStorageSystem(storageSystem.storageSystemId);
    }
  }

  loadStorageTypes() {
    this.dbLoading = true;
    this.catalogService.getStorageTypes()
      .subscribe(data => {
        data.forEach(element => {
          this.storageTypes.push(element);
        });
      }, error => {
        this.storageTypes = [];
        this.dbLoading = false;
      }, () => {
        this.storageTypes = this.storageTypes.sort((a, b): number => {
          return a.storageTypeName > b.storageTypeName ? 1 : -1;
        });
      });
    this.dbLoading = false;
  }

  loadStorageTypesForCategory(storageCategoryID: string) {
    this.dbLoading = true;
    this.storageTypes = [];
    this.catalogService.getStorageTypesForCategory(storageCategoryID)
      .subscribe(data => {
        data.forEach(element => {
          this.storageTypes.push(element);

        });
        this.dbLoading = false;
      }, error => {
        this.storageTypes = [];
        this.dbLoading = false;
      });
  }

  loadStorageTypeAttributesForType(storageTypeId: string, isStorageSystemLevel: string) {
    this.storageTypeAttributes = [];
    this.catalogService.getStorageTypeAttributes(storageTypeId, isStorageSystemLevel)
      .subscribe(data => {
        data.forEach(element => {
          this.storageTypeAttributes.push(element);
        });
      }, error => {
        this.storageTypeAttributes = [];
        this.snackbar.open('Invalid Storage Type', 'Dismiss', this.config.snackBarConfig);
      });
  }

  loadContainersForStorageSystem(storageSystemID: string) {
    this.dbLoading = true;
    this.containers = [];
    this.filteredContainers = [];
    this.catalogService.getContainersForSystem(storageSystemID)
      .subscribe(data => {
        data.forEach(element => {
          this.containers.push(element);
        });
        this.filteredContainers = this.containers;
        this.dbLoading = false;
      }, error => {
        this.filteredContainers = [];
        this.containers = [];
        this.dbLoading = false;
      });
  }

  loadObjectsForContainers(containerName: string, storageSystemId: string) {
    this.dbLoading = true;
    this.objects = [];
    this.filteredObjects = [];
    this.catalogService.getObjectsForContainerAndSystem(containerName, storageSystemId)
      .subscribe(data => {
        data.forEach(element => {
          this.objects.push(element);
        });
        this.filteredObjects = this.objects;
        this.dbLoading = false;
      }, error => {
        this.objects = [];
        this.filteredObjects = [];
        this.dbLoading = false;
      });
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
      });
    this.dbLoading = false;
  }

  loadStorageSystemsForType(storageTypeID: string) {
    this.dbLoading = true;
    this.storageSystems = [];
    this.catalogService.getStorageSystemsForType(storageTypeID)
      .subscribe(data => {
        data.forEach(element => {
          this.storageSystems.push(element);

        });
        this.dbLoading = false;
      }, error => {
        this.storageSystems = [];
        this.dbLoading = false;
      });
  }

  save() {
    this.submitting = true;
    const dataset: Dataset = this.populateDataset();
    this.catalogService.getUserByName(this.username)
      .subscribe(data => {
        if (data) {
          this.user = data;
          dataset.userId = this.user.userId;
          this.catalogService.insertDataset(dataset)
            .subscribe((resData) => {
              this.snackbar.open('Sucessfully created datasets ' + resData, 'Dismiss', this.config.snackBarConfig);
              this.submitting = false;
              this.dataSetAttributes = [];
              this.storageTypeAttributes = [];
              this.datasetForm.reset();
            }, (resError) => {
              this.snackbar.open('Dataset ' + dataset.storageDataSetName + ' is Duplicated', 'Dismiss', this.config.snackBarConfig);
              this.submitting = false;
              this.dataSetAttributes = [];
            });
        }
      }, error => {
        this.snackbar.open('Invalid User ID', 'Dismiss', this.config.snackBarConfig);
        this.submitting = false;
        this.dataSetAttributes = [];
      });
  }

  populateDataset() {
    const data: Dataset = new Dataset(this.datasetForm.controls.dataset.value, 0, '', null, 0, '', '', '', '', '', '', '', [], [], '');
    data.createdUser = this.username;
    data.storageDataSetName = this.datasetForm.controls.dataset.value;
    data.storageSystemId = this.datasetForm.controls.storageSystem.value.storageSystemId;
    data.storageContainerName = this.datasetForm.controls.container.value;
    data.clusters = this.allClusterIds;
    data.storageDataSetDescription = this.datasetForm.controls.description.value;
    data.objectName = this.datasetForm.controls.object.value;
    data.isAutoRegistered = 'N';
    return data;
  }

  validateDataset(event: Event) {
    if (this.formErrors.dataset) {
      return;
    }
    if (this.datasetForm.controls.dataset.value.length === 0) {
      this.formErrors.dataset = this.dataSetHint();
    }
  }

  validateDescription(event: Event) {
    if (this.formErrors.description) {
      return;
    }
    if (this.datasetForm.controls.description.value.length === 0) {
      this.formErrors.description = this.descriptionHint();
    }
  }

  descriptionHint(): string {
    return `Description should not be empty`;
  }

  dataSetHint(): string {
    return `Dataset name should not be empty`;
  }

  containerSelect(event: MdOptionSelectionChange, val: string) {
    if (event.source.selected) {
      const containerName = val;
      const storageSystem = this.datasetForm.controls.storageSystem.value;
      if (containerName && storageSystem) {
        this.loadObjectsForContainers(containerName, storageSystem.storageSystemId);
      }
    }
  }

  filterContainers(inputString = '') {
    if (inputString) {
      this.filteredContainers = this.containers.filter((val) => {
        return val.indexOf(inputString) >= 0;
      });
      if (this.filteredContainers.length === 0 && this.containers.length > 0) {
        this.datasetForm.controls.container.reset('');
      }
    } else {
      this.filteredContainers = this.containers;
    }
  }

  filterObjects(inputString = '') {
    if (inputString) {
      this.filteredObjects = this.objects.filter((val) => {
        return val.indexOf(inputString) >= 0;
      });
      if (this.filteredObjects.length === 0 && this.objects.length > 0) {
        this.datasetForm.controls.object.reset('');
      }
    } else {
      this.filteredObjects = this.objects;
    }
  }

  objectSelect(event: MdOptionSelectionChange, val: string) {
    if (event.source.selected) {
      this.objectName = val;
    }
  }

}
