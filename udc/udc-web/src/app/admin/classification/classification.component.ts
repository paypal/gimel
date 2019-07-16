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
import { Classification } from '../models/catalog-classification';
import { CatalogService } from '../../udc/catalog/services/catalog.service';
import { SessionService } from '../../core/services';
import { Page } from '../../udc/catalog/models/catalog-list-page';

@Component({
  selector: 'app-udc-admin-classification', templateUrl: './classification.component.html', styleUrls: ['./classification.component.scss'],
})
export class ClassificationComponent  implements OnInit {
  displayList = [];
  displayListClassification = [];
  loaded = true;
  projectName: string;
  projectLoaded = false;
  refresh = false;
  page = new Page();

  constructor(private catalogService: CatalogService, private sessionService: SessionService) {
    this.page.pageNumber = 0;
    this.page.size = 3;
  }

  ngOnInit() {
    this.projectLoaded = false;
    this.displayListClassification = ['All', 0, 1, 2, 3, 4, 5];
    this.projectName = 'All';
    this.projectLoaded = true;
  }

  disableDropDown() {
    this.loaded = false;
  }

  // enableDropDown() {
  //   this.loaded = true;
  // }

}
