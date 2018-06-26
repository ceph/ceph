import { CommonModule, DatePipe } from '@angular/common';
import { NgModule } from '@angular/core';

import { EmptyPipe } from '../empty.pipe';
import { CdDatePipe } from './cd-date.pipe';
import { CephReleaseNamePipe } from './ceph-release-name.pipe';
import { CephShortVersionPipe } from './ceph-short-version.pipe';
import { DimlessBinaryPipe } from './dimless-binary.pipe';
import { DimlessPipe } from './dimless.pipe';
import { EncodeUriPipe } from './encode-uri.pipe';
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
    CephReleaseNamePipe,
    RelativeDatePipe,
    ListPipe,
    FilterPipe,
    CdDatePipe,
    EmptyPipe,
    EncodeUriPipe
  ],
  exports: [
    DimlessBinaryPipe,
    HealthColorPipe,
    DimlessPipe,
    CephShortVersionPipe,
    CephReleaseNamePipe,
    RelativeDatePipe,
    ListPipe,
    FilterPipe,
    CdDatePipe,
    EmptyPipe,
    EncodeUriPipe
  ],
  providers: [
    DatePipe,
    CephShortVersionPipe,
    CephReleaseNamePipe,
    DimlessBinaryPipe,
    DimlessPipe,
    RelativeDatePipe,
    ListPipe,
    CdDatePipe,
    EmptyPipe,
    EncodeUriPipe
  ]
})
export class PipesModule {}
