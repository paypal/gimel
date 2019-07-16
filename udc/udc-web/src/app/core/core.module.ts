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
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { StoreModule } from '@ngrx/store';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { FlexLayoutModule } from '@angular/flex-layout';

import {
  ApiService,
  ConfigService,
  SessionService,
  IntrojsService,
} from './services';
// import { reduce } from './store';
import { reducers } from './store';
import { SharedModule } from '../shared/shared.module';

@NgModule({
  imports: [
    HttpModule,
    CommonModule,
    FormsModule,
    FlexLayoutModule,
    SharedModule,
    StoreModule.forRoot(reducers),
    StoreDevtoolsModule.instrument(),
  ],
  exports: [],
  providers: [
    ApiService,
    SessionService,
    ConfigService,
    IntrojsService,
  ],
})
export class CoreModule {
}
//
// entryComponents: [
//   ProdAuthDialogComponent,
//   AppStatusDialogComponent,
//   ConfirmDialogComponent,
//   TourDialogComponent,
//   FeedbackDialogComponent,
//   DeleteConfirmDialogComponent,
//   SwitchuiDialogComponent,
// ],
