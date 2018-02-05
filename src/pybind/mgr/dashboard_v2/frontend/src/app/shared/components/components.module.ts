import { NgModule } from '@angular/core';

import { DataTableModule } from './datatable/datatable.module';

@NgModule({
  imports: [
    DataTableModule
  ],
  declarations: [],
  providers: [],
  exports: [
    DataTableModule
  ]
})
export class ComponentsModule {}
