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

import { Component, OnInit } from '@angular/core';
import { CatalogService } from '../../udc/catalog/services/catalog.service';
import { Type } from '../models/catalog-type';
import { Entity } from '../models/catalog-entity';
import { Zone } from '../models/catalog-zone';
import { environment } from '../../../environments/environment';
import {SessionService} from '../../core/services/session.service';

@Component({
  selector: 'app-udc-admin-system', templateUrl: './catalog.system.component.html', styleUrls: ['./catalog.system.component.scss'],
})

export class SystemComponent implements OnInit {
  projectSystemList = [];
  displayList = [];
  loaded = true;
  projectName: string;
  projectLoaded = false;
  refresh = false;
  defaultType: Type = new Type();

  projectZoneList = [];
  displayZoneList = [];
  zloaded = true;
  zone: string;
  zonesLoaded = false;
  defaultZone: Zone = new Zone();
  refreshZoneList = false;

  projectEntityList = [];
  displayEntityList = [];
  eloaded = true;
  entity: string;
  entitiesLoaded = false;
  defaultEntity: Entity = new Entity();
  refreshEntityList = false;

  constructor(private catalogService: CatalogService, private sessionService: SessionService) {
  }

  loadEntities() {
    this.entitiesLoaded = false;
    this.defaultEntity.entityId = 0;
    this.defaultEntity.entityName = 'All';
    this.defaultEntity.entityDescription = 'All';
    this.projectEntityList.push(this.defaultEntity);
    this.catalogService.getEntityList().subscribe(data => {
      data.map(element => {
        this.projectEntityList.push(element);
      });
    }, error => {
      this.displayEntityList = this.projectEntityList = [];
      this.entitiesLoaded = true;
    }, () => {
      this.displayEntityList = this.projectEntityList.sort((a, b): number => {
        return a.entityName > b.entityName ? 1 : -1;
      });
      this.entity = this.defaultEntity.entityName;
      this.entitiesLoaded = true;
    });
  }
  loadZones() {
    this.zonesLoaded = false;
    this.defaultZone.zoneId = 0;
    this.defaultZone.zoneName = 'All';
    this.defaultZone.zoneDescription = 'All';
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
      this.zone = this.defaultZone.zoneName;
      this.zonesLoaded = true;
    });
  }

  ngOnInit() {
    this.loadZones();
    this.loadEntities();
    this.projectLoaded = false;
    this.defaultType.storageTypeId = 0;
    this.defaultType.storageTypeName = 'All';
    this.defaultType.storageTypeDescription = 'All';
    this.projectSystemList.push(this.defaultType);
    this.catalogService.getStorageTypes().subscribe(data => {
      data.map(element => {
        this.projectSystemList.push(element);
      });
    }, error => {
      this.displayList = this.projectSystemList = [];
      this.projectLoaded = true;
    }, () => {
      this.displayList = this.projectSystemList.sort((a, b): number => {
        return a.storageTypeName > b.storageTypeName ? 1 : -1;
      });
      this.projectName = this.defaultType.storageTypeName;
      this.projectLoaded = true;
    });
  }
  disableDropDown() {
    this.loaded = false;
  }

  enableDropDown() {
    this.loaded = true;
  }
  disableDropDownZone() {
    this.zloaded = false;
  }

  enableDropDownZone() {
    this.zloaded = true;
  }

  enableDropDownEntity() {
    this.eloaded = true;
  }
  disableDropDownEntity() {
    this.eloaded = false;
  }

  refreshProject() {
    this.refresh = !this.refresh;
    this.loaded = false;
  }

  refreshZone() {
    this.refreshZoneList = !this.refreshZoneList;
    this.zloaded = false;
  }

  refreshEntity() {
    this.refreshEntityList = !this.refreshEntityList;
    this.eloaded = false;
  }
}
