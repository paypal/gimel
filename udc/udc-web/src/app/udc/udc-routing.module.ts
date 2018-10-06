import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { CatalogHomeComponent } from './catalog/catalog-home/catalog.home.component';
import { CatalogRegisterDatasetComponent } from './catalog/catalog-register-dataset/catalog-register-dataset.component';
import {CatalogObjectHomeComponent} from './catalog/catalog-objects-home/catalog.object.home.component';
const udcRoutes: Routes = [

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
