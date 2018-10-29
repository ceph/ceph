import { CommonModule, DatePipe } from '@angular/common';
import { NgModule } from '@angular/core';

import { EmptyPipe } from '../empty.pipe';
import { CdDatePipe } from './cd-date.pipe';
import { CephReleaseNamePipe } from './ceph-release-name.pipe';
import { CephShortVersionPipe } from './ceph-short-version.pipe';
import { DimlessBinaryPerSecondPipe } from './dimless-binary-per-second.pipe';
import { DimlessBinaryPipe } from './dimless-binary.pipe';
import { DimlessPipe } from './dimless.pipe';
import { EncodeUriPipe } from './encode-uri.pipe';
import { FilterPipe } from './filter.pipe';
import { HealthColorPipe } from './health-color.pipe';
import { IopsPipe } from './iops.pipe';
import { ListPipe } from './list.pipe';
import { LogPriorityPipe } from './log-priority.pipe';
import { MillisecondsPipe } from './milliseconds.pipe';
import { OrdinalPipe } from './ordinal.pipe';
import { RelativeDatePipe } from './relative-date.pipe';
import { RoundPipe } from './round.pipe';

@NgModule({
  imports: [CommonModule],
  declarations: [
    DimlessBinaryPipe,
    DimlessBinaryPerSecondPipe,
    HealthColorPipe,
    DimlessPipe,
    CephShortVersionPipe,
    CephReleaseNamePipe,
    RelativeDatePipe,
    ListPipe,
    LogPriorityPipe,
    FilterPipe,
    CdDatePipe,
    EmptyPipe,
    EncodeUriPipe,
    RoundPipe,
    OrdinalPipe,
    MillisecondsPipe,
    IopsPipe
  ],
  exports: [
    DimlessBinaryPipe,
    DimlessBinaryPerSecondPipe,
    HealthColorPipe,
    DimlessPipe,
    CephShortVersionPipe,
    CephReleaseNamePipe,
    RelativeDatePipe,
    ListPipe,
    LogPriorityPipe,
    FilterPipe,
    CdDatePipe,
    EmptyPipe,
    EncodeUriPipe,
    RoundPipe,
    OrdinalPipe,
    MillisecondsPipe,
    IopsPipe
  ],
  providers: [
    DatePipe,
    CephShortVersionPipe,
    CephReleaseNamePipe,
    DimlessBinaryPipe,
    DimlessBinaryPerSecondPipe,
    DimlessPipe,
    RelativeDatePipe,
    ListPipe,
    LogPriorityPipe,
    CdDatePipe,
    EmptyPipe,
    EncodeUriPipe,
    OrdinalPipe,
    IopsPipe,
    MillisecondsPipe
  ]
})
export class PipesModule {}
