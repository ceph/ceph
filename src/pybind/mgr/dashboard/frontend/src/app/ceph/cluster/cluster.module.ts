import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

import {
  NgbDatepickerModule,
  NgbNavModule,
  NgbPopoverModule,
  NgbTimepickerModule,
  NgbTooltipModule,
  NgbTypeaheadModule
} from '@ng-bootstrap/ng-bootstrap';
import { TreeModule } from 'angular-tree-component';
import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';

import { SharedModule } from '../../shared/shared.module';
import { PerformanceCounterModule } from '../performance-counter/performance-counter.module';
import { CephSharedModule } from '../shared/ceph-shared.module';
import { ConfigurationDetailsComponent } from './configuration/configuration-details/configuration-details.component';
import { ConfigurationFormComponent } from './configuration/configuration-form/configuration-form.component';
import { ConfigurationComponent } from './configuration/configuration.component';
import { CrushmapComponent } from './crushmap/crushmap.component';
import { HostDetailsComponent } from './hosts/host-details/host-details.component';
import { HostFormComponent } from './hosts/host-form/host-form.component';
import { HostsComponent } from './hosts/hosts.component';
import { InventoryDevicesComponent } from './inventory/inventory-devices/inventory-devices.component';
import { InventoryComponent } from './inventory/inventory.component';
import { LogsComponent } from './logs/logs.component';
import { MgrModulesModule } from './mgr-modules/mgr-modules.module';
import { MonitorComponent } from './monitor/monitor.component';
import { OsdCreationPreviewModalComponent } from './osd/osd-creation-preview-modal/osd-creation-preview-modal.component';
import { OsdDetailsComponent } from './osd/osd-details/osd-details.component';
import { OsdDevicesSelectionGroupsComponent } from './osd/osd-devices-selection-groups/osd-devices-selection-groups.component';
import { OsdDevicesSelectionModalComponent } from './osd/osd-devices-selection-modal/osd-devices-selection-modal.component';
import { OsdFlagsModalComponent } from './osd/osd-flags-modal/osd-flags-modal.component';
import { OsdFormComponent } from './osd/osd-form/osd-form.component';
import { OsdListComponent } from './osd/osd-list/osd-list.component';
import { OsdPerformanceHistogramComponent } from './osd/osd-performance-histogram/osd-performance-histogram.component';
import { OsdPgScrubModalComponent } from './osd/osd-pg-scrub-modal/osd-pg-scrub-modal.component';
import { OsdRecvSpeedModalComponent } from './osd/osd-recv-speed-modal/osd-recv-speed-modal.component';
import { OsdReweightModalComponent } from './osd/osd-reweight-modal/osd-reweight-modal.component';
import { OsdScrubModalComponent } from './osd/osd-scrub-modal/osd-scrub-modal.component';
import { ActiveAlertListComponent } from './prometheus/active-alert-list/active-alert-list.component';
import { PrometheusTabsComponent } from './prometheus/prometheus-tabs/prometheus-tabs.component';
import { RulesListComponent } from './prometheus/rules-list/rules-list.component';
import { SilenceFormComponent } from './prometheus/silence-form/silence-form.component';
import { SilenceListComponent } from './prometheus/silence-list/silence-list.component';
import { SilenceMatcherModalComponent } from './prometheus/silence-matcher-modal/silence-matcher-modal.component';
import { ServiceDaemonListComponent } from './services/service-daemon-list/service-daemon-list.component';
import { ServiceDetailsComponent } from './services/service-details/service-details.component';
import { ServicesComponent } from './services/services.component';
import { TelemetryComponent } from './telemetry/telemetry.component';

@NgModule({
  imports: [
    CommonModule,
    PerformanceCounterModule,
    NgbNavModule,
    SharedModule,
    RouterModule,
    FormsModule,
    ReactiveFormsModule,
    NgbTooltipModule,
    MgrModulesModule,
    NgbTypeaheadModule,
    NgbTimepickerModule,
    TreeModule.forRoot(),
    NgBootstrapFormValidationModule,
    CephSharedModule,
    NgbDatepickerModule,
    NgbPopoverModule
  ],
  declarations: [
    HostsComponent,
    MonitorComponent,
    ConfigurationComponent,
    OsdListComponent,
    OsdDetailsComponent,
    OsdPerformanceHistogramComponent,
    OsdScrubModalComponent,
    OsdFlagsModalComponent,
    HostDetailsComponent,
    ConfigurationDetailsComponent,
    ConfigurationFormComponent,
    OsdReweightModalComponent,
    CrushmapComponent,
    LogsComponent,
    OsdRecvSpeedModalComponent,
    OsdPgScrubModalComponent,
    OsdRecvSpeedModalComponent,
    SilenceFormComponent,
    SilenceListComponent,
    SilenceMatcherModalComponent,
    ServicesComponent,
    InventoryComponent,
    HostFormComponent,
    OsdFormComponent,
    OsdDevicesSelectionModalComponent,
    InventoryDevicesComponent,
    OsdDevicesSelectionGroupsComponent,
    OsdCreationPreviewModalComponent,
    RulesListComponent,
    ActiveAlertListComponent,
    HostFormComponent,
    ServiceDetailsComponent,
    ServiceDaemonListComponent,
    TelemetryComponent,
    PrometheusTabsComponent
  ]
})
export class ClusterModule {}
