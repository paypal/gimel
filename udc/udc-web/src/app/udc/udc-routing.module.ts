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

import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { CatalogHomeComponent } from './catalog/catalog-home/catalog.home.component';
import { CatalogRegisterDatasetComponent } from './catalog/catalog-register-dataset/catalog-register-dataset.component';
import { CatalogObjectHomeComponent } from './catalog/catalog-objects-home/catalog.object.home.component';
import { UploadComponent } from '../admin/upload/upload.component';
const udcRoutes: Routes = [

  {
    path: 'datasets/:datasetName',
    component: CatalogHomeComponent,
    data: { title: 'Datasets Registered', page: 'cataloghome' },
  },
  {
    path: 'datasets',
    component: CatalogHomeComponent,
    data: { title: 'Datasets Registered', page: 'cataloghome' },
  },
  {
    path: 'register',
    component: CatalogRegisterDatasetComponent,
    data: { title: 'Register Dataset', page: 'registerdataset' },
  },
  {
    path: 'objects',
    component: CatalogObjectHomeComponent,
    data: { title: 'Objects Discovered', page: 'objectshome'},
  },
  {
    path: 'upload',
    component: UploadComponent,
    data: { title: 'Bulk Upload', page: 'upload'},
  }
];
@NgModule({
  imports: [
    RouterModule.forChild(udcRoutes),
  ],
  exports: [
    RouterModule,
  ],
})
export class UDCRoutingModule { }
