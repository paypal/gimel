import {CommonModule} from '@angular/common';
import {FlexLayoutModule} from '@angular/flex-layout';
import {MaterialModule} from '@angular/material';
import {NgModule} from '@angular/core';
import {NgxDatatableModule} from '@swimlane/ngx-datatable';
import {ReactiveFormsModule} from '@angular/forms';

import {UDCComponent} from './udc.component';
import {UDCRoutingModule} from './udc-routing.module';
import {SharedModule} from '../shared/shared.module';
import {
  CatalogHomeComponent
} from './catalog/catalog-home/catalog.home.component';
import {
  CatalogDatabaseListComponent
} from './catalog/catalog-dataset-list/catalog-dataset-list.component';
import {
  CatalogObjectListComponent
} from './catalog/catalog-object-list/catalog-object-list.component';
import {
  CatalogDatabaseActionComponent
} from './catalog/catalog-dataset-list/catalog-dataset-action/catalog-dataset-action.component';
import {
  CatalogDatabaseDetailComponent
} from './catalog/catalog-dataset-list/catalog-dataset-detail/catalog-dataset-detail.component';
import {CatalogService} from './catalog/services/catalog.service';
import {CatalogRegisterDatasetComponent} from './catalog/catalog-register-dataset/catalog-register-dataset.component';
import {CatalogDatasetEditDialogComponent} from './catalog/catalog-dataset-list/catalog-dataset-edit-dialog/catalog-dataset-edit-dialog.component';
import {CatalogDatabaseListPendingComponent} from './catalog/catalog-dataset-list-pending/catalog-dataset-list-pending.component';
import {CatalogDatabaseDetailPendingComponent} from './catalog/catalog-dataset-list-pending/catalog-dataset-detail-pending/catalog-dataset-detail-pending.component';
import {CatalogDatasetPendingEditDialogComponent} from './catalog/catalog-dataset-list-pending/catalog-dataset-pending-edit-dialog/catalog-dataset-pending-edit-dialog.component';
import {CatalogDatabaseActionPendingComponent} from './catalog/catalog-dataset-list-pending/catalog-dataset-action-pending/catalog-dataset-pending-action.component';
import {CatalogObjectHomeComponent} from './catalog/catalog-objects-home/catalog.object.home.component';
import {CatalogObjectDetailComponent} from './catalog/catalog-object-list/catalog-object-detail/catalog-object-detail.component';
import {CatalogObjectEditDialogComponent} from './catalog/catalog-dataset-list/catalog-object-edit-dialog/catalog-object-edit-dialog.component';
import {CatalogObjectActionComponent} from './catalog/catalog-object-list/catalog-object-action/catalog-object-action.component';
import {CatalogObjectEditComponent} from './catalog/catalog-object-list/catalog-object-edit-dialog/catalog-object-edit-dialog.component';
import {CatalogCreateObjectDialogComponent} from './catalog/catalog-object-create/catalog-object-create-dialog.component';
import {CatalogDatabaseDetailDeletedComponent} from './catalog/catalog-dataset-list-deleted/catalog-dataset-detail-deleted/catalog-dataset-detail-deleted.component';
import {CatalogDatabaseListDeletedComponent} from './catalog/catalog-dataset-list-deleted/catalog-dataset-list-deleted.component';
import {CatalogDatasetAccessDialogComponent} from './catalog/catalog-dataset-list/catalog-dataset-access-policy-dialog/catalog-dataset-access-dialog.component';
import {CatalogDatasetSampleDataDialogComponent} from './catalog/catalog-dataset-list/catalog-dataset-sample-data-dialog/catalog-dataset-sample-data-dialog.component';

@NgModule({
  imports: [CommonModule, FlexLayoutModule, UDCRoutingModule, MaterialModule, NgxDatatableModule, ReactiveFormsModule, SharedModule],
  declarations: [CatalogHomeComponent, CatalogObjectHomeComponent, UDCComponent, CatalogRegisterDatasetComponent, CatalogDatabaseListComponent, CatalogObjectListComponent, CatalogDatabaseListPendingComponent, CatalogDatabaseListDeletedComponent, CatalogDatabaseActionComponent, CatalogObjectActionComponent, CatalogDatabaseActionPendingComponent, CatalogDatabaseDetailComponent, CatalogObjectDetailComponent, CatalogDatabaseDetailPendingComponent, CatalogDatabaseDetailDeletedComponent, CatalogDatasetEditDialogComponent, CatalogDatasetAccessDialogComponent, CatalogCreateObjectDialogComponent, CatalogObjectEditDialogComponent, CatalogDatasetSampleDataDialogComponent, CatalogObjectEditComponent, CatalogDatasetPendingEditDialogComponent],
  providers: [CatalogService],
  entryComponents: [CatalogDatasetEditDialogComponent, CatalogDatasetAccessDialogComponent, CatalogDatasetSampleDataDialogComponent, CatalogObjectEditDialogComponent, CatalogObjectEditComponent, CatalogCreateObjectDialogComponent, CatalogDatasetPendingEditDialogComponent],
})
export class UdcModule {
}
