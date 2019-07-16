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

import { BrowserModule, Title } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { FlexLayoutModule } from '@angular/flex-layout';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { MatIconModule } from '@angular/material/icon';
import { AppRoutingModule } from './app-routing.module';
import { CoreModule } from './core/core.module';
import { SharedModule } from './shared/shared.module';
import { GlobalHeaderModule } from './global-header/global-header.module';
import { AppComponent } from './app.component';
import { HomeComponent } from './home/home.component';
import { UdcModule } from './udc/udc.module';
import { UdcHomeModule } from './udc-home/udc-home.module';
import { UdcAdminModule } from './admin/admin.module';
import { HttpClientModule } from '@angular/common/http';
import './rxjs-extensions';
import 'hammerjs';
import { DatasetDescriptionDialogComponent } from './dataset-description-dialog/dataset-description-dialog.component';
import { DescriptionModule } from './dataset-description-dialog/description.module';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

@NgModule({
  declarations: [AppComponent, HomeComponent, DatasetDescriptionDialogComponent],
  imports: [ FormsModule, ReactiveFormsModule, HttpClientModule, BrowserModule, BrowserAnimationsModule, MatIconModule, CoreModule, GlobalHeaderModule, SharedModule, AppRoutingModule, FlexLayoutModule, NgxDatatableModule, UdcHomeModule, UdcModule, UdcAdminModule, DescriptionModule],
  providers: [Title],
  bootstrap: [AppComponent],
  entryComponents: [DatasetDescriptionDialogComponent ],

})
export class AppModule {
}
