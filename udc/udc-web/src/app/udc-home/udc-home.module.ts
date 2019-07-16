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

import { CommonModule } from '@angular/common';
import { FlexLayoutModule } from '@angular/flex-layout';
import { NgModule } from '@angular/core';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { ReactiveFormsModule } from '@angular/forms';
import { UdcHomeComponent } from './udc-home.component';
import { UdcHomeRoutingModule } from './udc-home.routing.module';
import { MatListModule } from '@angular/material/list';
import { MatTabsModule } from '@angular/material/tabs';
import { SharedModule } from '../shared/shared.module';
import { HomeSearchComponent } from './basic-search/udc-home-search.component';
import { GhSearchboxResultComponent } from './basic-search/gh-searchbox/gh-searchbox-result.component';
import { GhSearchboxComponent } from './basic-search/gh-searchbox/gh-searchbox.component';
import { MatCardModule } from '@angular/material/card';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatSelectModule } from '@angular/material/select';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { OverlayModule } from '@angular/cdk/overlay';
import { MatButtonModule } from '@angular/material';

@NgModule({
  imports: [
    CommonModule,
    FlexLayoutModule,
    UdcHomeRoutingModule,
    NgxDatatableModule,
    ReactiveFormsModule,
    SharedModule,
    MatCardModule,
    MatProgressBarModule,
    MatSelectModule,
    MatIconModule,
    MatProgressSpinnerModule,
    OverlayModule,
    MatListModule,
    MatTabsModule,
    MatProgressSpinnerModule,
    MatButtonModule,
  ],
  declarations: [
    UdcHomeComponent,
    HomeSearchComponent,
    GhSearchboxComponent,
    GhSearchboxResultComponent,
  ],
})
export class UdcHomeModule { }
