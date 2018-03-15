import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';

import { SharedModule } from '../../shared/shared.module';
import {
  PerformanceCounterComponent
} from './performance-counter/performance-counter.component';
import { TablePerformanceCounterService } from './services/table-performance-counter.service';
import {
  TablePerformanceCounterComponent
} from './table-performance-counter/table-performance-counter.component';

@NgModule({
  imports: [
    CommonModule,
    SharedModule,
    RouterModule
  ],
  declarations: [
    TablePerformanceCounterComponent,
    PerformanceCounterComponent
  ],
  providers: [
    TablePerformanceCounterService
  ],
  exports: [
    TablePerformanceCounterComponent
  ]
})
export class PerformanceCounterModule { }
