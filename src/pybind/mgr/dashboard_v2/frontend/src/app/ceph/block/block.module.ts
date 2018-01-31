import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';

import { AlertModule, TabsModule } from 'ngx-bootstrap';

import { ComponentsModule } from '../../shared/components/components.module';
import { PipesModule } from '../../shared/pipes/pipes.module';
import { SharedModule } from '../../shared/shared.module';
import { PoolDetailComponent } from './pool-detail/pool-detail.component';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    TabsModule.forRoot(),
    AlertModule.forRoot(),
    SharedModule,
    ComponentsModule,
    PipesModule
  ],
  declarations: [PoolDetailComponent]
})
export class BlockModule { }
