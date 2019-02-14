import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { TabsModule } from 'ngx-bootstrap/tabs';
import { TypeaheadModule } from 'ngx-bootstrap/typeahead';

import { SharedModule } from '../../shared/shared.module';
import { NfsDetailsComponent } from './nfs-details/nfs-details.component';
import { NfsFormClientComponent } from './nfs-form-client/nfs-form-client.component';
import { NfsFormComponent } from './nfs-form/nfs-form.component';
import { NfsListComponent } from './nfs-list/nfs-list.component';

@NgModule({
  imports: [
    ReactiveFormsModule,
    RouterModule,
    SharedModule,
    TabsModule.forRoot(),
    CommonModule,
    TypeaheadModule.forRoot()
  ],
  declarations: [NfsListComponent, NfsDetailsComponent, NfsFormComponent, NfsFormClientComponent]
})
export class NfsModule {}
