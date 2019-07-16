/*
 * Copyright 2019 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {Component, OnInit} from '@angular/core';
import {AfterViewInit, OnDestroy, ViewChild} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {FormControl} from '@angular/forms';
import {ReplaySubject, Subject} from 'rxjs';
import {take, takeUntil} from 'rxjs/operators';
import {MatSelect} from '@angular/material';

import {CatalogService} from '../services/catalog.service';
import {StorageSystem} from '../models/catalog-storagesystem';
import {StorageType} from '../models/catalog-storagetype';
import {environment} from '../../../../environments/environment';
import {Zone} from '../../../admin/models/catalog-zone';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-home', templateUrl: './catalog.home.component.html', styleUrls: ['./catalog.home.component.scss'],
})

export class CatalogHomeComponent implements OnInit, OnDestroy {

  allOccurances = 'All';
  searchKeyword = 'Search';

  projectSystemList = [];
  projectTypeList = [];
  projectZoneList = [];
  projectContainerList = [];

  displayList = [];
  displayTypeList = [];
  displayZoneList = [];
  displayContainerList = [];

  systemName: string;
  typeName: string;
  zoneName: string;
  containerName: string;

  searchString: string;

  systemsLoaded = false;
  typesLoaded = false;
  zonesLoaded = false;
  containersLoaded = false;

  defaultSystem: StorageSystem = new StorageSystem();
  defaultType: StorageType = new StorageType();
  defaultZone: Zone = new Zone();
  defaultContainer: string;

  selectedType: string;
  selectedSystem: string;
  selectedZone: string;
  selectedContainer: string;


  allSystems: string;

  public storageTypesCtrl: FormControl = new FormControl();
  public systemsCtrl: FormControl = new FormControl();
  public zonesCtrl: FormControl = new FormControl();
  public containersCtrl: FormControl = new FormControl();

  public storageTypeFilterCtrl: FormControl = new FormControl();
  public systemFilterCtrl: FormControl = new FormControl();
  public zoneFilterCtrl: FormControl = new FormControl();
  public containerFilterCtrl: FormControl = new FormControl();

  public filteredStorageTypes: ReplaySubject<Array<any>> = new ReplaySubject<Array<any>>(1);
  public filteredSystems: ReplaySubject<Array<any>> = new ReplaySubject<Array<any>>(1);
  public filteredZones: ReplaySubject<Array<any>> = new ReplaySubject<Array<any>>(1);
  public filteredContainers: ReplaySubject<Array<any>> = new ReplaySubject<Array<any>>(1);

  @ViewChild('singleSelect') singleSelect: MatSelect;
  protected _onDestroy = new Subject<void>();

  datasetSize: number;
  datasetsByAttributesSize: number;
  datasetsByDescriptionSize: number;
  datasetsByTagSize: number;
  datasetsBySchemaSize: number;

  datasetFilter: string;
  schemaFilter: string;
  attributesFilter: string;
  tagFilter: string;
  descriptionFilter: string;

  isESEnabled: boolean;

  constructor(private route: ActivatedRoute, private catalogService: CatalogService, private sessionService: SessionService) {
    this.datasetFilter = 'DATASET';
    this.schemaFilter = 'OBJECT_SCHEMA';
    this.attributesFilter = 'OBJECT_ATTRIBUTE';
    this.tagFilter = 'TAG';
    this.descriptionFilter = 'DATASET_DESCRIPTION';

    this.searchString = '';

    this.selectedType = this.allOccurances;
    this.selectedZone = this.allOccurances;
    this.selectedSystem = this.allOccurances;
    this.selectedContainer = this.allOccurances;

    this.defaultType.storageTypeName = this.allOccurances;
    this.defaultType.storageTypeDescription = this.allOccurances;
    this.defaultType.storageTypeId = 0;

    this.defaultSystem.storageSystemId = 0;
    this.defaultSystem.storageSystemName = this.allOccurances;
    this.defaultSystem.storageSystemDescription = this.allOccurances;

    this.defaultZone.zoneId = 0;
    this.defaultZone.zoneName = this.allOccurances;
    this.defaultZone.zoneDescription = this.allOccurances;

    this.defaultContainer = this.allOccurances;

    this.isESEnabled = environment.enableES;
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
      this.storageTypesCtrl.setValue(this.displayTypeList[0]);
      this.filteredStorageTypes.next(this.displayTypeList.slice());
    });
    this.storageTypeFilterCtrl.valueChanges
      .pipe(takeUntil(this._onDestroy))
      .subscribe(() => {
        this.filterStorageTypes();
      });
  }

  loadZones() {
    this.zonesLoaded = false;
    this.projectZoneList.push(this.defaultZone);
    this.catalogService.getZonesList().subscribe(data => {
      data.map(element => {
        this.projectZoneList.push(element);
      });
    }, error => {
      this.displayZoneList = this.projectZoneList = [];
      this.zonesLoaded = true;
    }, () => {
      this.displayZoneList = this.projectZoneList.sort((a, b): number => {
        return a.zoneName > b.zoneName ? 1 : -1;
      });
      this.zoneName = this.defaultZone.zoneName;
      this.zonesLoaded = true;
      this.zonesCtrl.setValue(this.displayZoneList[0]);
      this.filteredZones.next(this.displayZoneList.slice());
    });
    this.zoneFilterCtrl.valueChanges
      .pipe(takeUntil(this._onDestroy))
      .subscribe(() => {
        this.filterZones();
      });
  }

  loadStorageSystems() {
    this.allSystems = '';
    this.systemsLoaded = false;
    this.projectSystemList.push(this.defaultSystem);
    this.catalogService.getStorageSystems().subscribe(data => {
      data.map(element => {
        this.projectSystemList.push(element);
      });
      this.allSystems = this.projectSystemList.filter(system => system.storageSystemName !== this.allOccurances).map(system => system.storageSystemName).join(',');
    }, error => {
      this.displayList = this.projectSystemList = [];
      this.systemsLoaded = true;
    }, () => {
      this.displayList = this.projectSystemList.sort((a, b): number => {
        return a.storageSystemName > b.storageSystemName ? 1 : -1;
      });
      this.systemName = this.defaultSystem.storageSystemName;
      this.systemsCtrl.setValue(this.displayList[0]);
      this.filteredSystems.next(this.displayList.slice());
      this.systemsLoaded = true;
    });
    this.systemFilterCtrl.valueChanges
      .pipe(takeUntil(this._onDestroy))
      .subscribe(() => {
        this.filterSystems();
      });
  }

  loadContainers() {
    this.containersLoaded = false;
    this.projectContainerList.push(this.defaultContainer);
    this.catalogService.getContainers().subscribe(data => {
      data.map(element => {
        this.projectContainerList.push(element);
      });
    }, error => {
      this.displayContainerList = this.projectContainerList = [];
      this.containersLoaded = true;
    }, () => {
      this.displayContainerList = this.projectContainerList.sort((a, b): number => {
        return a > b ? 1 : -1;
      });
      this.containerName = this.defaultContainer;
      this.containersCtrl.setValue(this.defaultContainer);
      this.filteredContainers.next(this.displayContainerList.slice());
      this.containersLoaded = true;
    });
    this.containerFilterCtrl.valueChanges
      .pipe(takeUntil(this._onDestroy))
      .subscribe(() => {
        this.filterContainers();
      });
  }


  loadContainersByFilter() {
    this.catalogService.getContainersForSystemList(this.allSystems).subscribe(containerData => {
      containerData.map(element => {
        this.projectContainerList.push(element);
      });
    }, errorContainers => {
      this.displayContainerList = this.projectContainerList = [];
      this.containersLoaded = true;
    }, () => {
      this.displayContainerList = this.projectContainerList.sort((a, b): number => {
        return a > b ? 1 : -1;
      });
      this.containerName = this.defaultContainer;
      this.containersLoaded = true;
      this.containersCtrl.setValue(this.defaultContainer);
      this.filteredContainers.next(this.displayContainerList.slice());
    });

    this.containerFilterCtrl.valueChanges
      .pipe(takeUntil(this._onDestroy))
      .subscribe(() => {
        this.filterContainers();
      });
  }


  onStorageTypeChange() {
    this.selectedType = this.typeName['storageTypeName'];
    this.allSystems = '';
    this.selectedSystem = this.defaultSystem.storageSystemName;
    this.selectedContainer = this.defaultContainer;
    this.projectSystemList = [];
    this.projectContainerList = [];
    this.projectSystemList.push(this.defaultSystem);
    this.projectContainerList.push(this.defaultContainer);
    this.systemsLoaded = false;
    this.containersLoaded = false;
    this.catalogService.getSystemList(this.selectedType).subscribe(data => {
      data.map(element => {
        this.projectSystemList.push(element);
      });
      this.allSystems = this.projectSystemList.filter(system => system.storageSystemName !== this.allOccurances).map(system => system.storageSystemName).join(',');
      this.loadContainersByFilter();
    }, error => {
      this.displayList = this.projectSystemList = [];
      this.systemsLoaded = true;
    }, () => {
      this.displayList = this.projectSystemList.sort((a, b): number => {
        return a.storageSystemName > b.storageSystemName ? 1 : -1;
      });
      this.systemName = this.defaultSystem.storageSystemName;
      this.systemsLoaded = true;
      this.systemsCtrl.setValue(this.displayList[0]);
      this.filteredSystems.next(this.displayList.slice());
    });
    this.systemFilterCtrl.valueChanges
      .pipe(takeUntil(this._onDestroy))
      .subscribe(() => {
        this.filterSystems();
      });

  }

  onZoneChange() {
    this.selectedZone = this.zoneName['zoneName'];
    this.allSystems = '';
    this.selectedSystem = this.defaultSystem.storageSystemName;
    this.selectedContainer = this.defaultContainer;
    this.projectSystemList = [];
    this.projectContainerList = [];
    this.projectSystemList.push(this.defaultSystem);
    this.projectContainerList.push(this.defaultContainer);
    this.systemsLoaded = false;
    this.containersLoaded = false;
    this.catalogService.getSystemListForZoneAndType(this.selectedZone, this.selectedType).subscribe(data => {
      data.map(element => {
        this.projectSystemList.push(element);
      });
      if (this.projectSystemList.length > 1) {
        this.allSystems = this.projectSystemList.filter(system => system.storageSystemName !== this.allOccurances).map(system => system.storageSystemName).join(',');
      } else {
        this.allSystems = 'Unknown';
      }
      this.loadContainersByFilter();
    }, error => {
      this.displayList = this.projectSystemList = [];
      this.systemsLoaded = true;
    }, () => {
      this.displayList = this.projectSystemList.sort((a, b): number => {
        return a.storageSystemName > b.storageSystemName ? 1 : -1;
      });
      this.systemName = this.defaultSystem.storageSystemName;
      this.systemsLoaded = true;
      this.systemsCtrl.setValue(this.displayList[0]);
      this.filteredSystems.next(this.displayList.slice());
    });
    this.systemFilterCtrl.valueChanges
      .pipe(takeUntil(this._onDestroy))
      .subscribe(() => {
        this.filterSystems();
      });

  }

  onSystemChange() {
    this.selectedSystem = this.systemName['storageSystemName'];
    this.selectedContainer = this.defaultContainer;
    this.projectContainerList = [];
    this.projectContainerList.push(this.defaultContainer);
    this.containersLoaded = false;
    if (this.selectedSystem === this.allOccurances) {
      if (this.selectedType === this.allOccurances && this.selectedZone === this.allOccurances) {
        this.allSystems = this.projectSystemList.filter(system => system.storageSystemName !== this.allOccurances).map(system => system.storageSystemName).join(',');
      } else if (this.selectedType !== this.allOccurances && this.selectedZone === this.allOccurances) {
        const selectedTypeObject = this.projectTypeList.find(type => type.storageTypeName === this.selectedType);
        const typeId = selectedTypeObject.storageTypeId;
        this.allSystems = this.projectSystemList.filter(system => system.storageSystemName !== this.allOccurances).filter(system => system.storageTypeId === typeId).map(system => system.storageSystemName).join(',');
      } else if (this.selectedType === this.allOccurances && this.selectedZone !== this.allOccurances) {
        const selectedZoneObject = this.projectZoneList.find(zone => zone.zoneName === this.selectedZone);
        const zoneId = selectedZoneObject.zoneId;
        this.allSystems = this.projectSystemList.filter(system => system.storageSystemName !== this.allOccurances).filter(system => system.zoneId === zoneId).map(system => system.storageSystemName).join(',');
      } else {
        const selectedTypeObject = this.projectTypeList.find(type => type.storageTypeName === this.selectedType);
        const typeId = selectedTypeObject.storageTypeId;
        const selectedZoneObject = this.projectZoneList.find(zone => zone.zoneName === this.selectedZone);
        const zoneId = selectedZoneObject.zoneId;
        this.allSystems = this.projectSystemList.filter(system => system.storageSystemName !== this.allOccurances).filter(system => system.zoneId === zoneId && system.storageTypeId === typeId).map(system => system.storageSystemName).join(',');
      }
    } else {
      this.allSystems = this.selectedSystem;
    }
    this.loadContainersByFilter();
  }

  onContainerChange() {
    this.selectedContainer = this.containerName;
  }

  receivedataSetSize($event) {
    this.datasetSize = $event;
  }

  receivedatasetsBySchemaSize($event) {
    this.datasetsBySchemaSize = $event;
  }

  receivedatasetsByAttributesSize($event) {
    this.datasetsByAttributesSize = $event;
  }

  receivedatasetsByTagSize($event) {
    this.datasetsByTagSize = $event;
  }

  receivedatasetsByDescriptionSize($event) {
    this.datasetsByDescriptionSize = $event;
  }

  ngOnInit() {
    this.route.paramMap.subscribe(params => {
      this.searchString = params.get('datasetName');
    });
    if (this.searchString == undefined) {
      this.searchString = '';
    }
    this.loadStorageTypes();
    this.loadStorageSystems();
    this.loadZones();
    this.loadContainers();

  }


  ngOnDestroy() {
    this._onDestroy.next();
    this._onDestroy.complete();
  }

  protected filterStorageTypes() {
    if (!this.displayTypeList) {
      return;
    }
    let search = this.storageTypeFilterCtrl.value;
    if (!search) {
      this.filteredStorageTypes.next(this.displayTypeList.slice());
      return;
    } else {
      search = search.toLowerCase();
    }

    this.filteredStorageTypes.next(this.displayTypeList.filter(a => a.storageTypeName.toLowerCase().indexOf(search) > -1));
  }

  protected filterZones() {
    if (!this.displayZoneList) {
      return;
    }
    let search = this.zoneFilterCtrl.value;
    if (!search) {
      this.filteredZones.next(this.displayZoneList.slice());
      return;
    } else {
      search = search.toLowerCase();
    }
    this.filteredZones.next(this.displayZoneList.filter(a => a.zoneName.toLowerCase().indexOf(search) > -1));
  }

  protected filterSystems() {
    if (!this.displayList) {
      return;
    }
    let search = this.systemFilterCtrl.value;
    if (!search) {
      this.filteredSystems.next(this.displayList.slice());
      return;
    } else {
      search = search.toLowerCase();
    }
    this.filteredSystems.next(this.displayList.filter(a => a.storageSystemName.toLowerCase().indexOf(search) > -1));
  }


  protected filterContainers() {
    if (!this.displayContainerList) {
      return;
    }
    let search = this.containerFilterCtrl.value;
    if (!search) {
      this.filteredContainers.next(this.displayContainerList.slice());
      return;
    } else {
      search = search.toLowerCase();
    }
    this.filteredContainers.next(this.displayContainerList.filter(a => a.indexOf(search) > -1));
  }
}
