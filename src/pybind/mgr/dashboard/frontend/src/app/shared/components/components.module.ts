import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { AlertModule, PopoverModule } from 'ngx-bootstrap';

import { HelperComponent } from './helper/helper.component';
import { SparklineComponent } from './sparkline/sparkline.component';
import { ViewCacheComponent } from './view-cache/view-cache.component';

@NgModule({
  imports: [
    CommonModule,
    AlertModule.forRoot(),
    PopoverModule.forRoot(),
    ChartsModule
  ],
  declarations: [
    ViewCacheComponent,
    SparklineComponent,
    HelperComponent
  ],
  providers: [],
  exports: [
    ViewCacheComponent,
    SparklineComponent,
    HelperComponent
  ]
})
export class ComponentsModule { }
