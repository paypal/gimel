import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { HealthMetricsComponent } from './metrics/health-metrics.component';

const healthRoutes: Routes = [{
  path: 'metrics',
  component: HealthMetricsComponent,
  data: {title: 'Health Check', page: 'healthmetrics'},
}];

@NgModule({
  imports: [
    RouterModule.forChild(healthRoutes),
  ],
  exports: [
    RouterModule,
  ],
})
export class HealthRoutingModule { }
