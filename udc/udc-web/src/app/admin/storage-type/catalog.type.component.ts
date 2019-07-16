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
import { StorageSystem } from '../../udc/catalog/models/catalog-storagesystem';
import { CatalogService } from '../../udc/catalog/services/catalog.service';
import { Category } from '../models/catalog-category';
import { environment } from '../../../environments/environment';
import {SessionService} from '../../core/services/session.service';

@Component({
  selector: 'app-catalog-home', templateUrl: './catalog.type.component.html', styleUrls: ['./catalog.type.component.scss'],
})

export class TypeComponent implements OnInit {
  projectList = [];
  displayList = [];
  loaded = true;
  projectName: string;
  projectLoaded = false;
  refresh = false;
  defaultSystem: Category = new Category();

  constructor(private catalogService: CatalogService, private sessionService: SessionService) {
  }

  ngOnInit() {
    this.projectLoaded = false;

    this.defaultSystem.storageId = 0;
    this.defaultSystem.storageName = 'All';
    this.defaultSystem.storageDescription = 'All';
    this.projectList.push(this.defaultSystem);
    this.catalogService.getStorageCategories().subscribe(data => {
      data.map(element => {
        this.projectList.push(element);
      });
    }, error => {
      this.displayList = this.projectList = [];
      this.projectLoaded = true;
    }, () => {
      this.displayList = this.projectList.sort((a, b): number => {
        return a.storageName > b.storageName ? 1 : -1;
      });
      this.projectName = this.defaultSystem.storageName;
      this.projectLoaded = true;
    });
  }

  disableDropDown() {
    this.loaded = false;
  }

  enableDropDown() {
    this.loaded = true;
  }

  refreshProject() {
    this.refresh = !this.refresh;
    this.loaded = false;
  }

}
