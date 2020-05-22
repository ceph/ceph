import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';

import { SharedModule } from '../../shared/shared.module';
import { Nfs501Component } from './nfs-501/nfs-501.component';
import { NfsDetailsComponent } from './nfs-details/nfs-details.component';
import { NfsFormClientComponent } from './nfs-form-client/nfs-form-client.component';
import { NfsFormComponent } from './nfs-form/nfs-form.component';
import { NfsListComponent } from './nfs-list/nfs-list.component';

@NgModule({
  imports: [
    ReactiveFormsModule,
    RouterModule,
    SharedModule,
    NgbNavModule,
    CommonModule,
    NgbTypeaheadModule,
    NgBootstrapFormValidationModule
  ],
  declarations: [
    NfsListComponent,
    NfsDetailsComponent,
    NfsFormComponent,
    NfsFormClientComponent,
    Nfs501Component
  ],
  exports: [Nfs501Component]
})
export class NfsModule {}
