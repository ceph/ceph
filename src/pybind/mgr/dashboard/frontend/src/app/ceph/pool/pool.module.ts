import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import { BsDropdownModule, PopoverModule, TabsModule } from 'ngx-bootstrap';

import { ServicesModule } from '../../shared/services/services.module';
import { SharedModule } from '../../shared/shared.module';
import { PoolFormComponent } from './pool-form/pool-form.component';
import { PoolListComponent } from './pool-list/pool-list.component';

@NgModule({
  imports: [
    CommonModule,
    TabsModule,
    PopoverModule.forRoot(),
    SharedModule,
    RouterModule,
    ReactiveFormsModule,
    BsDropdownModule,
    ServicesModule
  ],
  exports: [PoolListComponent, PoolFormComponent],
  declarations: [PoolListComponent, PoolFormComponent]
})
export class PoolModule {}
