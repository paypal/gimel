import {CommonModule} from '@angular/common';
import {FlexLayoutModule} from '@angular/flex-layout';
import {MaterialModule} from '@angular/material';
import {NgModule} from '@angular/core';
import {NgxDatatableModule} from '@swimlane/ngx-datatable';
import {ReactiveFormsModule} from '@angular/forms';

import {UdcAdminComponent} from './admin.component';
import {UdcAdminRoutingModule} from './admin.routing.module';
import {SharedModule} from '../shared/shared.module';
import {UserComponent} from './user/user.component';
import {ClusterComponent} from './cluster/cluster.component';
import {CatalogClusterListComponent} from './cluster/catalog-cluster-list/catalog-cluster-list.component';
import {CatalogCreateClusterDialogComponent} from './cluster/catalog-cluster-create/catalog-cluster-create-dialog.component';
import {CatalogClusterEditDialogComponent} from './cluster/catalog-cluster-edit-dialog/catalog-cluster-edit-dialog.component';
import {CatalogClusterActionComponent} from './cluster/catalog-cluster-action/catalog-cluster-action.component';
import {ZoneComponent} from './zone/zone.component';
import {CatalogZoneListComponent} from './zone/catalog-zone-list/catalog-zone-list.component';
import {CatalogCreateZoneDialogComponent} from './zone/catalog-zone-create/catalog-zone-create-dialog.component';
import {CatalogZoneEditDialogComponent} from './zone/catalog-zone-edit-dialog/catalog-zone-edit-dialog.component';
import {CatalogZoneActionComponent} from './zone/catalog-zone-action/catalog-zone-action.component';
import {CatalogUserEditDialogComponent} from './user/catalog-user-edit-dialog/catalog-user-edit-dialog.component';
import {CatalogCreateUserDialogComponent} from './user/catalog-user-create/catalog-user-create-dialog.component';
import {CatalogUserListComponent} from './user/catalog-user-list/catalog-user-list.component';
import {CatalogUserActionComponent} from './user/catalog-user-action/catalog-user-action.component';
import {CategoryComponent} from './storage-category/category.component';
import {CatalogCategoryListComponent} from './storage-category/catalog-category-list/catalog-category-list.component';
import {CatalogCreateCategoryDialogComponent} from './storage-category/catalog-category-create/catalog-category-create-dialog.component';
import {CatalogCategoryActionComponent} from './storage-category/catalog-category-action/catalog-category-action.component';
import {CatalogCategoryEditDialogComponent} from './storage-category/catalog-category-edit-dialog/catalog-category-edit-dialog.component';
import {TypeComponent} from './storage-type/catalog.type.component';
import {CatalogTypeListComponent} from './storage-type/catalog-type-list/catalog-type-list.component';
import {CatalogTypeActionComponent} from './storage-type/catalog-type-action/catalog-type-action.component';
import {CatalogTypeEditDialogComponent} from './storage-type/catalog-type-edit-dialog/catalog-type-edit-dialog.component';
import {CatalogCreateTypeDialogComponent} from './storage-type/catalog-type-create/catalog-type-create-dialog.component';
import {SystemComponent} from './storage-system/catalog.system.component';
import {CatalogSystemListComponent} from './storage-system/catalog-system-list/catalog-system-list.component';
import {CatalogCreateSystemDialogComponent} from './storage-system/catalog-system-create/catalog-system-create-dialog.component';
import {CatalogSystemActionComponent} from './storage-system/catalog-system-action/catalog-system-action.component';
import {CatalogTypeViewAttributesDialogComponent} from './storage-type/catalog-type-view-attributes-dialog/catalog-type-view-attributes-dialog.component';
import {CatalogSystemViewAttributesDialogComponent} from './storage-system/catalog-system-view-attributes-dialog/catalog-system-view-attributes-dialog.component';
import {CatalogSystemEditDialogComponent} from './storage-system/catalog-system-edit-dialog/catalog-system-edit-dialog.component';

@NgModule({
  imports: [CommonModule, FlexLayoutModule, UdcAdminRoutingModule, MaterialModule, NgxDatatableModule, ReactiveFormsModule, SharedModule,],
  declarations: [UdcAdminComponent, UserComponent, ClusterComponent, ZoneComponent, CategoryComponent, TypeComponent, SystemComponent, CatalogClusterListComponent, CatalogZoneListComponent, CatalogCategoryListComponent, CatalogTypeListComponent, CatalogSystemListComponent, CatalogUserListComponent, CatalogCreateClusterDialogComponent, CatalogCreateZoneDialogComponent, CatalogCreateCategoryDialogComponent, CatalogCreateTypeDialogComponent, CatalogCreateSystemDialogComponent, CatalogCreateUserDialogComponent, CatalogClusterActionComponent, CatalogZoneActionComponent, CatalogUserActionComponent, CatalogCategoryActionComponent, CatalogTypeActionComponent, CatalogSystemActionComponent, CatalogClusterEditDialogComponent, CatalogZoneEditDialogComponent, CatalogUserEditDialogComponent, CatalogCategoryEditDialogComponent, CatalogTypeEditDialogComponent, CatalogTypeViewAttributesDialogComponent, CatalogSystemViewAttributesDialogComponent, CatalogSystemEditDialogComponent,],
  entryComponents: [CatalogCreateClusterDialogComponent, CatalogCreateZoneDialogComponent, CatalogCreateCategoryDialogComponent, CatalogCreateUserDialogComponent, CatalogCreateTypeDialogComponent, CatalogCreateSystemDialogComponent, CatalogClusterEditDialogComponent, CatalogZoneEditDialogComponent, CatalogUserEditDialogComponent, CatalogCategoryEditDialogComponent, CatalogTypeEditDialogComponent, CatalogTypeViewAttributesDialogComponent, CatalogSystemViewAttributesDialogComponent, CatalogSystemEditDialogComponent,],
})
export class UdcAdminModule {
}
