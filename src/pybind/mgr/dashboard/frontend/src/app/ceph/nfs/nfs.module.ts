import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { NgbNavModule, NgbTooltipModule, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
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
    NgbTooltipModule
  ],
  exports: [NfsListComponent, NfsFormComponent, NfsDetailsComponent],
  declarations: [NfsListComponent, NfsDetailsComponent, NfsFormComponent, NfsFormClientComponent]
})
export class NfsModule {}
