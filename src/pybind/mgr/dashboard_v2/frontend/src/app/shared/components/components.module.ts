import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { TableComponent } from './table/table.component';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { TableDetailsDirective } from './table/table-details.directive';
import {FormsModule} from '@angular/forms';

@NgModule({
  entryComponents: [],
  imports: [CommonModule, NgxDatatableModule, FormsModule],
  declarations: [TableComponent, TableDetailsDirective],
  exports: [TableComponent, NgxDatatableModule]
})
export class ComponentsModule {}
