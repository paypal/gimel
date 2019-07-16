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
import { UserComponent } from './user/user.component';
import { ClusterComponent } from './cluster/cluster.component';
import { ZoneComponent } from './zone/zone.component';
import { CategoryComponent } from './storage-category/category.component';
import { TypeComponent } from './storage-type/catalog.type.component';
import { SystemComponent } from './storage-system/catalog.system.component';
import { EntityComponent } from './entity/entity.component';
import { NotificationComponent } from './notification/notification.component';
import { ClassificationComponent } from './classification/classification.component';
import { UploadComponent } from './upload/upload.component';

const udcHomeRoutes: Routes = [

  {
    path: 'user',
    component: UserComponent,
    data: { title: 'User Configuration', page: 'user' },
  },
  {
    path: 'cluster',
    component: ClusterComponent,
    data: { title: 'Cluster Configuration', page: 'cluster'},
  },
  {
    path: 'zone',
    component: ZoneComponent,
    data: { title: 'Zone Configuration', page: 'zone'},
  },
  {
    path: 'category',
    component: CategoryComponent,
    data: { title: 'Datastore Category Configuration', page: 'category'},
  },
  {
    path: 'entity',
    component: EntityComponent,
    data: { title: 'Entity Configuration', page: 'entity'},
  },
  {
    path: 'classification',
    component: ClassificationComponent,
    data: { title: 'Manage Classification', page: 'classification'},
  },
  {
    path: 'type',
    component: TypeComponent,
    data: { title: 'Datastore Type Configuration', page: 'type'},
  },
  {
    path: 'system',
    component: SystemComponent,
    data: { title: 'Datastore System Configuration', page: 'system'},
  },
  {
    path: 'notification',
    component: NotificationComponent,
    data: { title: 'Notification Configuration', page: 'notification'},
  },
];
@NgModule({
  imports: [
    RouterModule.forChild(udcHomeRoutes),
  ],
  exports: [
    RouterModule,
  ],
})
export class UdcAdminRoutingModule { }
