import {Component, OnInit} from '@angular/core';

import {CatalogService} from '../services/catalog.service';
import {StorageSystem} from '../models/catalog-storagesystem';
import {StorageType} from '../models/catalog-storagetype';

@Component({
  selector: 'app-catalog-home',
  templateUrl: './catalog.home.component.html',
  styleUrls: ['./catalog.home.component.scss'],
})

export class CatalogHomeComponent implements OnInit {
  projectSystemList = [];
  projectTypeList = [];
  displayList = [];
  displayTypeList = [];
  sloaded = true;
  tloaded = true;
  systemName: string;
  typeName: string;
  systemsLoaded = false;
  typesLoaded = false;
  refreshSystem = false;
  defaultSystem: StorageSystem = new StorageSystem();
  defaultType: StorageType = new StorageType();

  constructor(private catalogService: CatalogService) {
    this.defaultType.storageTypeName = 'All';
    this.defaultType.storageTypeDescription = 'All';
    this.defaultType.storageTypeId = 0;
    this.defaultSystem.storageSystemId = 0;
    this.defaultSystem.storageSystemName = 'All';
    this.defaultSystem.storageSystemDescription = 'All';
  }

  loadStorageTypes() {
    this.typesLoaded = false;
    this.projectTypeList.push(this.defaultType);
    this.catalogService.getStorageTypes().subscribe(data => {
      data.map(element => {
        this.projectTypeList.push(element);
      });
    }, error => {
      this.displayTypeList = this.projectTypeList = [];
      this.typesLoaded = true;
    }, () => {
      this.displayTypeList = this.projectTypeList.sort((a, b): number => {
        return a.storageTypeName > b.storageTypeName ? 1 : -1;
      });
      this.typeName = this.defaultType.storageTypeName;
      this.typesLoaded = true;
    });
  }

  loadStorageSystems() {
    this.systemsLoaded = false;
    this.projectSystemList.push(this.defaultSystem);
    this.catalogService.getStorageSystems().subscribe(data => {
      data.map(element => {
        this.projectSystemList.push(element);
      });
    }, error => {
      this.displayList = this.projectSystemList = [];
      this.systemsLoaded = true;
    }, () => {
      this.displayList = this.projectSystemList.sort((a, b): number => {
        return a.storageSystemName > b.storageSystemName ? 1 : -1;
      });
      this.enableDropDownType();
      this.systemName = this.defaultSystem.storageSystemName;
      this.systemsLoaded = true;
    });
  }

  onStorageTypeChange() {
    this.projectSystemList = [];
    this.projectSystemList.push(this.defaultSystem);
    this.systemsLoaded = false;
    this.catalogService.getSystemList(this.typeName).subscribe(data => {
      data.map(element => {
        this.projectSystemList.push(element);
      });
    }, error => {
      this.displayList = this.projectSystemList = [];
      this.systemsLoaded = true;
    }, () => {
      this.displayList = this.projectSystemList.sort((a, b): number => {
        return a.storageSystemName > b.storageSystemName ? 1 : -1;
      });
      this.enableDropDownType();
      this.systemName = this.defaultSystem.storageSystemName;
      this.systemsLoaded = true;
    });
  }


  ngOnInit() {
    this.loadStorageTypes();
    this.loadStorageSystems();
  }

  disableDropDownSystem() {
    this.sloaded = false;
  }

  enableDropDownSystem() {
    this.sloaded = true;
  }

  disableDropDownType() {
    this.tloaded = false;
  }

  enableDropDownType() {
    this.tloaded = true;
  }

  refreshProject() {
    this.refreshSystem = !this.refreshSystem;
    this.sloaded = false;
  }
}
