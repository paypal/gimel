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

import { CommonModule, DatePipe} from '@angular/common';
import { FlexLayoutModule } from '@angular/flex-layout';
import { NgModule } from '@angular/core';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { ReactiveFormsModule } from '@angular/forms';
import { UdcAdminComponent } from './admin.component';
import { UdcAdminRoutingModule } from './admin.routing.module';
import { SharedModule } from '../shared/shared.module';
import { UserComponent } from './user/user.component';
import { ClusterComponent } from './cluster/cluster.component';
import { EntityComponent } from './entity/entity.component';
import { CatalogEntityActionComponent } from './entity/catalog-entity-action/catalog-entity-action.component';
import { CatalogCreateEntityDialogComponent } from './entity/catalog-entity-create/catalog-entity-create-dialog.component';
import { CatalogEntityListComponent } from './entity/catalog-entity-list/catalog-entity-list.component';
import { CatalogEntityEditDialogComponent } from './entity/catalog-entity-edit-dialog/catalog-entity-edit-dialog.component';
import { CatalogClusterListComponent } from './cluster/catalog-cluster-list/catalog-cluster-list.component';
import { CatalogCreateClusterDialogComponent } from './cluster/catalog-cluster-create/catalog-cluster-create-dialog.component';
import { CatalogClusterEditDialogComponent } from './cluster/catalog-cluster-edit-dialog/catalog-cluster-edit-dialog.component';
import { CatalogClusterActionComponent } from './cluster/catalog-cluster-action/catalog-cluster-action.component';
import { ZoneComponent } from './zone/zone.component';
import { CatalogZoneListComponent } from './zone/catalog-zone-list/catalog-zone-list.component';
import { CatalogCreateZoneDialogComponent } from './zone/catalog-zone-create/catalog-zone-create-dialog.component';
import { CatalogZoneEditDialogComponent } from './zone/catalog-zone-edit-dialog/catalog-zone-edit-dialog.component';
import { CatalogZoneActionComponent } from './zone/catalog-zone-action/catalog-zone-action.component';
import { CatalogUserEditDialogComponent } from './user/catalog-user-edit-dialog/catalog-user-edit-dialog.component';
import { CatalogCreateUserDialogComponent } from './user/catalog-user-create/catalog-user-create-dialog.component';
import { CatalogUserListComponent } from './user/catalog-user-list/catalog-user-list.component';
import { CatalogUserActionComponent } from './user/catalog-user-action/catalog-user-action.component';
import { CategoryComponent } from './storage-category/category.component';
import { CatalogCategoryListComponent } from './storage-category/catalog-category-list/catalog-category-list.component';
import { CatalogCreateCategoryDialogComponent } from './storage-category/catalog-category-create/catalog-category-create-dialog.component';
import { CatalogCategoryActionComponent } from './storage-category/catalog-category-action/catalog-category-action.component';
import { CatalogCategoryEditDialogComponent } from './storage-category/catalog-category-edit-dialog/catalog-category-edit-dialog.component';
import { TypeComponent } from './storage-type/catalog.type.component';
import { CatalogTypeListComponent } from './storage-type/catalog-type-list/catalog-type-list.component';
import { CatalogTypeActionComponent } from './storage-type/catalog-type-action/catalog-type-action.component';
import { CatalogTypeEditDialogComponent } from './storage-type/catalog-type-edit-dialog/catalog-type-edit-dialog.component';
import { CatalogCreateTypeDialogComponent } from './storage-type/catalog-type-create/catalog-type-create-dialog.component';
import { SystemComponent } from './storage-system/catalog.system.component';
import { CatalogSystemListComponent } from './storage-system/catalog-system-list/catalog-system-list.component';
import { CatalogCreateSystemDialogComponent } from './storage-system/catalog-system-create/catalog-system-create-dialog.component';
import { CatalogSystemActionComponent } from './storage-system/catalog-system-action/catalog-system-action.component';
import { CatalogTypeViewAttributesDialogComponent } from './storage-type/catalog-type-view-attributes-dialog/catalog-type-view-attributes-dialog.component';
import { CatalogSystemViewAttributesDialogComponent } from './storage-system/catalog-system-view-attributes-dialog/catalog-system-view-attributes-dialog.component';
import { CatalogClusterViewAttributesDialogComponent } from './cluster/catalog-cluster-view-attributes-dialog/catalog-cluster-view-attributes-dialog.component';
import { CatalogSystemEditDialogComponent } from './storage-system/catalog-system-edit-dialog/catalog-system-edit-dialog.component';
import { NotificationComponent } from './notification/notification.component';
import { NotificationListComponent } from './notification/notification-list/notification-list.component';
import { NotificationActionComponent } from './notification/notification-action/notification-action.component';
import { NotificationDialogComponent } from './notification/notification-create/notification-create-dialog.component';
import { NotificationUploadDialogComponent } from './notification/notification-upload/notification-upload-dialog.component';
import { DescriptionUploadDialogComponent } from './upload/description-upload/description-upload-dialog.component';
import { OwnershipUploadDialogComponent } from './upload/ownership-upload/ownership-upload-dialog.component';
import { NotificationEditDialogComponent } from './notification/notification-edit-dialog/notification-edit-dialog.component';
import { NotificationDetailDialogComponent } from './notification/notification-detail/notification-detail.component';
import { ExcelService } from './notification/services/download-excel-service';
import { MatListModule } from '@angular/material/list';
import { MatGridListModule } from '@angular/material/grid-list';
import { MatTabsModule } from '@angular/material/tabs';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatIconModule } from '@angular/material/icon';
import { CdkStepperModule } from '@angular/cdk/stepper';
import { CdkTableModule } from '@angular/cdk/table';
import { MatStepperModule } from '@angular/material/stepper';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatMenuModule } from '@angular/material/menu';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { Ng2GoogleChartsModule } from 'ng2-google-charts';
import { CatalogSystemDiscoveryMetricsDialogComponent } from './storage-system/catalog-system-discovery-dialog/catalog-system-discovery-metrics-dialog.component';
import { NgxSpinnerModule } from 'ngx-spinner';
import { ClassificationComponent } from './classification/classification.component';
import { CatalogClassificationListComponent } from './classification/catalog-classification-list/catalog-classification-list.component';
import { CatalogCreateClassificationDialogComponent } from './classification/catalog-classification-create/catalog-classification-create-dialog.component';
import { CatalogClassificationActionComponent } from './classification/catalog-classification-action/catalog-classification-action.component';
import { CatalogClassificationEditDialogComponent } from './classification/catalog-classification-edit-dialog/catalog-classification-edit-dialog.component';
import { MatPaginatorModule, MatProgressSpinnerModule } from '@angular/material';
import { ClassificationUploadDialogComponent } from './upload/classification-upload/classification-upload-dialog.component';


@NgModule({
  imports: [CdkStepperModule, CdkTableModule, MatStepperModule, MatPaginatorModule, NgxSpinnerModule, Ng2GoogleChartsModule, MatTooltipModule, MatButtonModule, MatChipsModule, MatMenuModule, MatInputModule, MatProgressBarModule, MatIconModule, MatSelectModule, MatFormFieldModule, MatCardModule, MatTabsModule, CommonModule, FlexLayoutModule, UdcAdminRoutingModule, NgxDatatableModule, ReactiveFormsModule, SharedModule, MatGridListModule, MatListModule, MatProgressSpinnerModule],
  declarations: [ClassificationUploadDialogComponent, CatalogClassificationEditDialogComponent, CatalogClassificationActionComponent, CatalogCreateClassificationDialogComponent, ClassificationComponent, CatalogClassificationListComponent, CatalogEntityEditDialogComponent, CatalogEntityActionComponent, CatalogCreateEntityDialogComponent, CatalogEntityListComponent, EntityComponent, UdcAdminComponent, UserComponent, ClusterComponent, ZoneComponent, CategoryComponent, TypeComponent, SystemComponent, CatalogClusterListComponent, CatalogZoneListComponent, CatalogCategoryListComponent, CatalogTypeListComponent, CatalogSystemListComponent, CatalogUserListComponent, CatalogCreateClusterDialogComponent, CatalogCreateZoneDialogComponent, CatalogCreateCategoryDialogComponent, CatalogCreateTypeDialogComponent, CatalogCreateSystemDialogComponent, CatalogCreateUserDialogComponent, CatalogClusterActionComponent, CatalogZoneActionComponent, CatalogUserActionComponent, CatalogCategoryActionComponent, CatalogTypeActionComponent, CatalogSystemActionComponent, CatalogClusterEditDialogComponent, CatalogZoneEditDialogComponent, CatalogUserEditDialogComponent, CatalogCategoryEditDialogComponent, CatalogTypeEditDialogComponent, CatalogClusterViewAttributesDialogComponent, CatalogTypeViewAttributesDialogComponent, CatalogSystemViewAttributesDialogComponent, CatalogSystemViewAttributesDialogComponent, CatalogSystemDiscoveryMetricsDialogComponent, CatalogSystemEditDialogComponent, NotificationComponent, NotificationListComponent, NotificationActionComponent, NotificationDialogComponent, NotificationEditDialogComponent, NotificationUploadDialogComponent, NotificationDetailDialogComponent, DescriptionUploadDialogComponent, OwnershipUploadDialogComponent],
  providers: [DatePipe, ExcelService],
  entryComponents: [ClassificationUploadDialogComponent, CatalogClassificationEditDialogComponent, CatalogCreateClassificationDialogComponent, CatalogEntityEditDialogComponent, CatalogCreateEntityDialogComponent, CatalogCreateClusterDialogComponent, CatalogCreateZoneDialogComponent, CatalogCreateCategoryDialogComponent, CatalogCreateUserDialogComponent, CatalogCreateTypeDialogComponent, CatalogCreateSystemDialogComponent, CatalogClusterEditDialogComponent, CatalogZoneEditDialogComponent, CatalogUserEditDialogComponent, CatalogCategoryEditDialogComponent, CatalogTypeEditDialogComponent, CatalogClusterViewAttributesDialogComponent, CatalogTypeViewAttributesDialogComponent, CatalogSystemViewAttributesDialogComponent, CatalogSystemViewAttributesDialogComponent, CatalogSystemDiscoveryMetricsDialogComponent, CatalogSystemEditDialogComponent, NotificationDialogComponent, NotificationEditDialogComponent, NotificationUploadDialogComponent, DescriptionUploadDialogComponent, OwnershipUploadDialogComponent],
})
export class UdcAdminModule {
}
