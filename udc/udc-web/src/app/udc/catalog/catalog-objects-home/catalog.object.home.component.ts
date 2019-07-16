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

import { CatalogService } from '../services/catalog.service';
import { StorageSystem } from '../models/catalog-storagesystem';
import { StorageType } from '../models/catalog-storagetype';
import { environment } from '../../../../environments/environment';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-object-home',
  templateUrl: './catalog.object.home.component.html',
  styleUrls: ['./catalog.object.home.component.scss'],
})

export class CatalogObjectHomeComponent implements OnInit {
  projectContainerList = [];
  projectSystemList = [];
  displayContainerList = [];
  displaySystemList = [];
  sloaded = true;
  cloaded = true;
  systemName: string;
  containerName: string;
  systemsLoaded = false;
  containersLoaded = false;
  refreshContainer = false;
  defaultSystem: StorageSystem = new StorageSystem();
  defaultContainer: string;
  systemMap: Map<string, StorageSystem>;
  systemMapById: Map<number, StorageSystem>;

  constructor(private catalogService: CatalogService, private sessionService: SessionService) {
    this.defaultSystem.storageSystemId = 0;
    this.defaultSystem.storageSystemName = 'All';
    this.defaultSystem.storageSystemDescription = 'All';
    this.defaultContainer = 'All';
    this.systemMap = new Map();
    this.systemMapById = new Map();
  }

  loadStorageSystems() {
    this.systemsLoaded = false;
    this.projectSystemList.push(this.defaultSystem);
    this.catalogService.getStorageSystems().subscribe(data => {
      data.map(element => {
        this.projectSystemList.push(element);
      });
    }, error => {
      this.displaySystemList = this.projectSystemList = [];
      this.systemsLoaded = true;
    }, () => {
      this.displaySystemList = this.projectSystemList.sort((a, b): number => {
        return a.storageSystemName > b.storageSystemName ? 1 : -1;
      });
      this.displaySystemList.forEach(system => {
        this.systemMap.set(system.storageSystemName, system);
        this.systemMapById.set(system.storageSystemId, system);
      });
      this.systemName = this.defaultSystem.storageSystemName;
      this.systemsLoaded = true;
    });
  }

  onStorageSystemChange() {
    this.projectContainerList = [];
    this.projectContainerList.push(this.defaultContainer);
    this.containersLoaded = false;
    const system: StorageSystem = this.systemMap.get(this.systemName);
    this.catalogService.getContainersForSystem(system.storageSystemId.toString()).subscribe(data => {
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
      this.enableDropDownSystem();
      this.containerName = this.defaultContainer;
      this.containersLoaded = true;
    });
  }

  ngOnInit() {
    this.loadStorageSystems();
  }

  disableDropDownSystem() {
    this.sloaded = false;
  }

  refreshProject() {
    this.refreshContainer = !this.refreshContainer;
    this.cloaded = false;
  }

  enableDropDownSystem() {
    this.sloaded = true;
  }

}
