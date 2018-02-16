import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';

import { TabsModule } from 'ngx-bootstrap';

import { ComponentsModule } from '../../shared/components/components.module';
import { PipesModule } from '../../shared/pipes/pipes.module';
import { ServicesModule } from '../../shared/services/services.module';
import { SharedModule } from '../../shared/shared.module';
import { IscsiComponent } from './iscsi/iscsi.component';
import { PoolDetailComponent } from './pool-detail/pool-detail.component';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    TabsModule.forRoot(),
    SharedModule,
    ComponentsModule,
    PipesModule,
    ServicesModule
  ],
  declarations: [
    PoolDetailComponent,
    IscsiComponent
  ]
})
export class BlockModule { }
