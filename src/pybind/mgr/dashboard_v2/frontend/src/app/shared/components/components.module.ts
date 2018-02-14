import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { AlertModule } from 'ngx-bootstrap';

import { SparklineComponent } from './sparkline/sparkline.component';
import { ViewCacheComponent } from './view-cache/view-cache.component';

@NgModule({
  imports: [
    CommonModule,
    AlertModule.forRoot(),
    ChartsModule
  ],
  declarations: [
    ViewCacheComponent,
    SparklineComponent
  ],
  providers: [],
  exports: [
    ViewCacheComponent,
    SparklineComponent
  ]
})
export class ComponentsModule { }
