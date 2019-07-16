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
import { ReactiveFormsModule } from '@angular/forms';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { StandaloneRoutingModule } from './standalone-routing.module';
import { StandaloneStageProfileComponent } from './dataset/standalone-dataset.component';
import { CatalogService } from '../udc/catalog/services/catalog.service';
import { SharedModule } from '../shared/shared.module';
import { MatAutocompleteModule, MatButtonModule, MatCardModule, MatChipsModule, MatDialogModule, MatFormFieldModule, MatGridListModule, MatIconModule, MatInputModule, MatListModule, MatMenuModule, MatPaginatorModule, MatProgressBarModule, MatProgressSpinnerModule, MatSelectModule, MatSortModule, MatTableModule, MatTabsModule, MatTooltipModule, MatExpansionModule } from '@angular/material';
import { OverlayModule } from '@angular/cdk/overlay';
import { SatPopoverModule } from '@ncstate/sat-popover';
import { InlineEditComponent } from './dataset/inline-edit/inline-edit.component';
import { CatalogDescriptionEditDialogComponent } from './dataset/description-edit-dialog/catalog-description-edit-dialog.component';
import { SessionService } from '../core/services/session.service';
import { CatalogDatasetTimelineDialogComponent } from './catalog-dataset-timeline/timeline-dialog/catalog-dataset-timeline-dialog';
import { CatalogDatabaseTimelineComponent } from './catalog-dataset-timeline/catalog-dataset-timeline.component';
import { MglTimelineModule } from 'angular-mgl-timeline';
import { CatalogOwnershipClaimDialogComponent } from './dataset/ownership-claim-dialog/catalog-ownership-claim-dialog.component';
import { NgxMatSelectSearchModule } from 'ngx-mat-select-search';
@NgModule({
  imports: [ NgxMatSelectSearchModule, MglTimelineModule, MatDialogModule, MatExpansionModule, SatPopoverModule, MatTableModule, MatSortModule, MatPaginatorModule, OverlayModule, MatChipsModule, MatProgressSpinnerModule, MatTooltipModule, MatButtonModule, MatGridListModule, MatListModule, MatMenuModule, MatAutocompleteModule, MatProgressBarModule, MatFormFieldModule, MatInputModule, MatCardModule, MatTabsModule, MatIconModule, MatSelectModule, CommonModule, FormsModule, ReactiveFormsModule, StandaloneRoutingModule, NgxDatatableModule, SharedModule],
  declarations: [CatalogDatabaseTimelineComponent, CatalogDatasetTimelineDialogComponent, InlineEditComponent, StandaloneStageProfileComponent, CatalogDescriptionEditDialogComponent, CatalogOwnershipClaimDialogComponent],
  providers: [CatalogService, SessionService],
  entryComponents: [ CatalogDatasetTimelineDialogComponent, CatalogDescriptionEditDialogComponent, CatalogOwnershipClaimDialogComponent],
})
export class StandaloneModule {
}
