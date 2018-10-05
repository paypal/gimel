import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';
import {UserComponent} from './user/user.component';
import {ClusterComponent} from './cluster/cluster.component';
import {ZoneComponent} from './zone/zone.component';
import {CategoryComponent} from './storage-category/category.component';
import {TypeComponent} from './storage-type/catalog.type.component';
import {SystemComponent} from './storage-system/catalog.system.component';

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
    path: 'type',
    component: TypeComponent,
    data: { title: 'Datastore Type Configuration', page: 'type'},
  },
  {
    path: 'system',
    component: SystemComponent,
    data: { title: 'Datastore System Configuration', page: 'system'},
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
