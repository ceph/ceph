import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { AlertModule } from 'ngx-bootstrap';
import { DataTableModule } from './datatable/datatable.module';

import { ViewCacheComponent } from './view-cache/view-cache.component';

@NgModule({
  imports: [
    CommonModule,
    DataTableModule,
    AlertModule.forRoot()
  ],
  declarations: [ViewCacheComponent],
  providers: [],
  exports: [
    DataTableModule,
    ViewCacheComponent
  ]
})
export class ComponentsModule { }
