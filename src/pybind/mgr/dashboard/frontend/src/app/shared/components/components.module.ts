import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { AlertModule, PopoverModule, TooltipModule } from 'ngx-bootstrap';

import { PipesModule } from '../pipes/pipes.module';
import { HelperComponent } from './helper/helper.component';
import { SparklineComponent } from './sparkline/sparkline.component';
import { SubmitButtonComponent } from './submit-button/submit-button.component';
import { UsageBarComponent } from './usage-bar/usage-bar.component';
import { ViewCacheComponent } from './view-cache/view-cache.component';

@NgModule({
  imports: [
    CommonModule,
    AlertModule.forRoot(),
    PopoverModule.forRoot(),
    TooltipModule.forRoot(),
    ChartsModule,
    PipesModule
  ],
  declarations: [
    ViewCacheComponent,
    SparklineComponent,
    HelperComponent,
    SubmitButtonComponent,
    UsageBarComponent
  ],
  providers: [],
  exports: [
    ViewCacheComponent,
    SparklineComponent,
    HelperComponent,
    SubmitButtonComponent,
    UsageBarComponent
  ]
})
export class ComponentsModule { }
