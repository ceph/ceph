import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { SharedModule } from '../../shared/shared.module';
import { TablePerformanceCounterService } from './services/table-performance-counter.service';
import { TablePerformanceCounterComponent } from './table-performance-counter/table-performance-counter.component'; // tslint:disable-line

@NgModule({
  imports: [
    CommonModule,
    SharedModule
  ],
  declarations: [
    TablePerformanceCounterComponent
  ],
  providers: [
    TablePerformanceCounterService
  ],
  exports: [
    TablePerformanceCounterComponent
  ]
})
export class PerformanceCounterModule { }
