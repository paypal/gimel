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

import { UDCComponent } from './udc.component';
import { UDCRoutingModule } from './udc-routing.module';
import { SharedModule } from '../shared/shared.module';
import { CatalogHomeComponent } from './catalog/catalog-home/catalog.home.component';
import { CatalogDatabaseListComponent } from './catalog/catalog-dataset-list/catalog-dataset-list.component';
import { CatalogObjectListComponent } from './catalog/catalog-object-list/catalog-object-list.component';
import { CatalogDatabaseActionComponent } from './catalog/catalog-dataset-list/catalog-dataset-action/catalog-dataset-action.component';
import { CatalogDatabaseDetailComponent } from './catalog/catalog-dataset-list/catalog-dataset-detail/catalog-dataset-detail.component';
import { CatalogService } from './catalog/services/catalog.service';
import { CatalogRegisterDatasetComponent } from './catalog/catalog-register-dataset/catalog-register-dataset.component';
import { CatalogDatasetEditDialogComponent } from './catalog/catalog-dataset-list/catalog-dataset-edit-dialog/catalog-dataset-edit-dialog.component';
import { CatalogObjectHomeComponent } from './catalog/catalog-objects-home/catalog.object.home.component';
import { CatalogObjectDetailComponent } from './catalog/catalog-object-list/catalog-object-detail/catalog-object-detail.component';
import { CatalogObjectEditDialogComponent } from './catalog/catalog-dataset-list/catalog-object-edit-dialog/catalog-object-edit-dialog.component';
import { CatalogObjectActionComponent } from './catalog/catalog-object-list/catalog-object-action/catalog-object-action.component';
import { CatalogObjectEditComponent } from './catalog/catalog-object-list/catalog-object-edit-dialog/catalog-object-edit-dialog.component';
import { CatalogCreateObjectDialogComponent } from './catalog/catalog-object-create/catalog-object-create-dialog.component';
import { CatalogDatasetAccessDialogComponent } from './catalog/catalog-dataset-list/catalog-dataset-access-policy-dialog/catalog-dataset-access-dialog.component';
import { CatalogCreateObjectAttributeDialogComponent } from './catalog/catalog-object-attribute-create/catalog-object-attribute-create-dialog.component';
import { RecursiveSearchboxComponent } from './catalog/catalog-home/recursive-search/recursive-searchbox.component';
import { RecursiveSearchboxResultComponent } from './catalog/catalog-home/recursive-search/recursive-searchbox-result.component';
import { NgxSpinnerModule } from 'ngx-spinner';
import { OverlayModule } from '@angular/cdk/overlay';
import { MatSelectModule } from '@angular/material/select';
import { MatIconModule } from '@angular/material/icon';
import { MatTabsModule } from '@angular/material/tabs';
import { MatCardModule } from '@angular/material/card';
import { MatInputModule } from '@angular/material/input';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatMenuModule } from '@angular/material/menu';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { MatListModule } from '@angular/material/list';
import { MatGridListModule } from '@angular/material/grid-list';
import { MatButtonModule } from '@angular/material/button';
import { MatTooltipModule } from '@angular/material/tooltip';
import { SessionService } from '../core/services/session.service';
import { MatTableModule, MatSortModule } from '@angular/material';
import { MatPaginatorModule } from '@angular/material/paginator';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatChipsModule } from '@angular/material/chips';
import { MatDialogModule } from '@angular/material';
import { CatalogDatasetDetailDialogComponent } from './catalog/catalog-dataset-list/catalog-dataset-detail-dialog/catalog-dataset-detail-dialog.component';
import { NgxMatSelectSearchModule } from 'ngx-mat-select-search';
import { DescriptionModule } from '../dataset-description-dialog/description.module';
import { UploadComponent } from '../admin/upload/upload.component';

@NgModule({
  imports: [NgxMatSelectSearchModule, NgxSpinnerModule, MatDialogModule, MatChipsModule, MatToolbarModule, MatTableModule, MatPaginatorModule, MatSortModule, MatTooltipModule, MatButtonModule, MatGridListModule, MatListModule, MatMenuModule, MatAutocompleteModule, MatProgressBarModule, MatFormFieldModule, MatInputModule, MatCardModule, MatTabsModule, MatIconModule, MatSelectModule, CommonModule, FlexLayoutModule, UDCRoutingModule, NgxDatatableModule, ReactiveFormsModule, SharedModule, DescriptionModule, OverlayModule, MatProgressSpinnerModule],
  declarations: [  UploadComponent, CatalogCreateObjectAttributeDialogComponent, CatalogHomeComponent, CatalogObjectHomeComponent, UDCComponent, CatalogRegisterDatasetComponent, CatalogDatabaseListComponent, CatalogObjectListComponent, CatalogDatabaseActionComponent, CatalogObjectActionComponent, CatalogDatabaseDetailComponent, CatalogObjectDetailComponent, CatalogDatasetEditDialogComponent, CatalogDatasetAccessDialogComponent, CatalogCreateObjectDialogComponent, CatalogObjectEditDialogComponent, CatalogObjectEditComponent,  CatalogDatasetDetailDialogComponent, RecursiveSearchboxComponent, RecursiveSearchboxResultComponent],
  providers: [CatalogService, SessionService],
  entryComponents: [  CatalogDatasetDetailDialogComponent, CatalogDatabaseDetailComponent, CatalogCreateObjectAttributeDialogComponent, CatalogDatasetEditDialogComponent, CatalogDatasetAccessDialogComponent, CatalogObjectEditDialogComponent, CatalogObjectEditComponent, CatalogCreateObjectDialogComponent],
})
export class UdcModule {
}
