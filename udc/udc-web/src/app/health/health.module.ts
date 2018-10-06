import { NgModule } from '@angular/core';
import { HealthMetricsComponent } from './metrics/health-metrics.component';
import { HealthRoutingModule } from './health-routing.module';
import { HealthCheckModule } from '../healthcheck';
import { CommonModule } from '@angular/common';

@NgModule({
  imports: [
    CommonModule,
    HealthCheckModule,
    HealthRoutingModule,
  ],
  declarations: [
    HealthMetricsComponent,
  ],
})
export class HealthModule { }
