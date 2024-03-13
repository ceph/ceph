import { CommonModule, DatePipe } from '@angular/common';
import { NgModule } from '@angular/core';

import { ArrayPipe } from './array.pipe';
import { BooleanTextPipe } from './boolean-text.pipe';
import { BooleanPipe } from './boolean.pipe';
import { CdDatePipe } from './cd-date.pipe';
import { CephReleaseNamePipe } from './ceph-release-name.pipe';
import { CephShortVersionPipe } from './ceph-short-version.pipe';
import { DimlessBinaryPerSecondPipe } from './dimless-binary-per-second.pipe';
import { DimlessBinaryPipe } from './dimless-binary.pipe';
import { DimlessPipe } from './dimless.pipe';
import { DurationPipe } from './duration.pipe';
import { EmptyPipe } from './empty.pipe';
import { EncodeUriPipe } from './encode-uri.pipe';
import { FilterPipe } from './filter.pipe';
import { HealthColorPipe } from './health-color.pipe';
import { HealthIconPipe } from './health-icon.pipe';
import { HealthLabelPipe } from './health-label.pipe';
import { IopsPipe } from './iops.pipe';
import { IscsiBackstorePipe } from './iscsi-backstore.pipe';
import { JoinPipe } from './join.pipe';
import { LogPriorityPipe } from './log-priority.pipe';
import { MapPipe } from './map.pipe';
import { MdsSummaryPipe } from './mds-summary.pipe';
import { MgrSummaryPipe } from './mgr-summary.pipe';
import { MillisecondsPipe } from './milliseconds.pipe';
import { NotAvailablePipe } from './not-available.pipe';
import { OrdinalPipe } from './ordinal.pipe';
import { OsdSummaryPipe } from './osd-summary.pipe';
import { RbdConfigurationSourcePipe } from './rbd-configuration-source.pipe';
import { RelativeDatePipe } from './relative-date.pipe';
import { RoundPipe } from './round.pipe';
import { SanitizeHtmlPipe } from './sanitize-html.pipe';
import { SearchHighlightPipe } from './search-highlight.pipe';
import { TruncatePipe } from './truncate.pipe';
import { UpperFirstPipe } from './upper-first.pipe';
import { OctalToHumanReadablePipe } from './octal-to-human-readable.pipe';
import { PathPipe } from './path.pipe';
import { PluralizePipe } from './pluralize.pipe';

@NgModule({
  imports: [CommonModule],
  declarations: [
    ArrayPipe,
    BooleanPipe,
    BooleanTextPipe,
    DimlessBinaryPipe,
    DimlessBinaryPerSecondPipe,
    HealthColorPipe,
    HealthLabelPipe,
    DimlessPipe,
    CephShortVersionPipe,
    CephReleaseNamePipe,
    RelativeDatePipe,
    IscsiBackstorePipe,
    JoinPipe,
    LogPriorityPipe,
    FilterPipe,
    CdDatePipe,
    EmptyPipe,
    EncodeUriPipe,
    RoundPipe,
    OrdinalPipe,
    MillisecondsPipe,
    NotAvailablePipe,
    IopsPipe,
    UpperFirstPipe,
    RbdConfigurationSourcePipe,
    DurationPipe,
    MapPipe,
    TruncatePipe,
    SanitizeHtmlPipe,
    SearchHighlightPipe,
    HealthIconPipe,
    MgrSummaryPipe,
    MdsSummaryPipe,
    OsdSummaryPipe,
    OctalToHumanReadablePipe,
    PathPipe,
    PluralizePipe
  ],
  exports: [
    ArrayPipe,
    BooleanPipe,
    BooleanTextPipe,
    DimlessBinaryPipe,
    DimlessBinaryPerSecondPipe,
    HealthColorPipe,
    HealthLabelPipe,
    DimlessPipe,
    CephShortVersionPipe,
    CephReleaseNamePipe,
    RelativeDatePipe,
    IscsiBackstorePipe,
    JoinPipe,
    LogPriorityPipe,
    FilterPipe,
    CdDatePipe,
    EmptyPipe,
    EncodeUriPipe,
    RoundPipe,
    OrdinalPipe,
    MillisecondsPipe,
    NotAvailablePipe,
    IopsPipe,
    UpperFirstPipe,
    RbdConfigurationSourcePipe,
    DurationPipe,
    MapPipe,
    TruncatePipe,
    SanitizeHtmlPipe,
    SearchHighlightPipe,
    HealthIconPipe,
    MgrSummaryPipe,
    MdsSummaryPipe,
    OsdSummaryPipe,
    OctalToHumanReadablePipe,
    PathPipe,
    PluralizePipe
  ],
  providers: [
    ArrayPipe,
    BooleanPipe,
    BooleanTextPipe,
    DatePipe,
    CephShortVersionPipe,
    CephReleaseNamePipe,
    DimlessBinaryPipe,
    DimlessBinaryPerSecondPipe,
    DimlessPipe,
    RelativeDatePipe,
    IscsiBackstorePipe,
    JoinPipe,
    LogPriorityPipe,
    CdDatePipe,
    EmptyPipe,
    EncodeUriPipe,
    OrdinalPipe,
    IopsPipe,
    MillisecondsPipe,
    NotAvailablePipe,
    UpperFirstPipe,
    DurationPipe,
    MapPipe,
    TruncatePipe,
    SanitizeHtmlPipe,
    HealthIconPipe,
    MgrSummaryPipe,
    MdsSummaryPipe,
    OsdSummaryPipe,
    OctalToHumanReadablePipe
  ]
})
export class PipesModule {}
