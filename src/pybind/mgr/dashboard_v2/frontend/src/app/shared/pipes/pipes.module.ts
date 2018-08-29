import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { CephShortVersionPipe } from './ceph-short-version.pipe';
import { DimlessBinaryPipe } from './dimless-binary.pipe';
import { DimlessPipe } from './dimless.pipe';
import { FilterPipe } from './filter.pipe';
import { HealthColorPipe } from './health-color.pipe';
import { ListPipe } from './list.pipe';
import { RelativeDatePipe } from './relative-date.pipe';

@NgModule({
  imports: [CommonModule],
  declarations: [
    DimlessBinaryPipe,
    HealthColorPipe,
    DimlessPipe,
    CephShortVersionPipe,
    RelativeDatePipe,
    ListPipe,
    FilterPipe
  ],
  exports: [
    DimlessBinaryPipe,
    HealthColorPipe,
    DimlessPipe,
    CephShortVersionPipe,
    RelativeDatePipe,
    ListPipe,
    FilterPipe
  ],
  providers: [
    CephShortVersionPipe,
    DimlessBinaryPipe,
    DimlessPipe,
    RelativeDatePipe,
    ListPipe
  ]
})
export class PipesModule {}
