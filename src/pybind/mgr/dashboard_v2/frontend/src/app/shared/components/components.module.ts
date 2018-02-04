import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';

import {NgxDatatableModule} from '@swimlane/ngx-datatable';

import { TableDetailsDirective } from './table/table-details.directive';
import { TableComponent } from './table/table.component';

@NgModule({
  entryComponents: [],
  imports: [CommonModule, NgxDatatableModule, FormsModule],
  declarations: [TableComponent, TableDetailsDirective],
  exports: [TableComponent, NgxDatatableModule]
})
export class ComponentsModule {}
