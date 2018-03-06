import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';

import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { ComponentsModule } from '../../shared/components/components.module';
import { PipesModule } from '../../shared/pipes/pipes.module';
import { ServicesModule } from '../../shared/services/services.module';
import { SharedModule } from '../../shared/shared.module';
import { IscsiComponent } from './iscsi/iscsi.component';
import { MirrorHealthColorPipe } from './mirror-health-color.pipe';
import { MirroringComponent } from './mirroring/mirroring.component';
import { PoolDetailComponent } from './pool-detail/pool-detail.component';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    TabsModule.forRoot(),
    ProgressbarModule.forRoot(),
    SharedModule,
    ComponentsModule,
    PipesModule,
    ServicesModule
  ],
  declarations: [
    PoolDetailComponent,
    IscsiComponent,
    MirroringComponent,
    MirrorHealthColorPipe
  ]
})
export class BlockModule { }
