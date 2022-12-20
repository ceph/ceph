import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { Observable, Subscription } from 'rxjs';
import { take } from 'rxjs/operators';

import { ClusterService } from '~/app/shared/api/cluster.service';
import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { HealthService } from '~/app/shared/api/health.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { DashboardDetails } from '~/app/shared/models/cd-details';
import { Permissions } from '~/app/shared/models/permissions';
import { AlertmanagerAlert } from '~/app/shared/models/prometheus-alerts';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { SummaryService } from '~/app/shared/services/summary.service';

@Component({
  selector: 'cd-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent implements OnInit, OnDestroy {
  detailsCardData: DashboardDetails = {};
  osdSettingsService: any;
  osdSettings: any;
  interval = new Subscription();
  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;
  color: string;
  capacityService: any;
  capacity: any;
  healthData$: Observable<Object>;
  prometheusAlerts$: Observable<AlertmanagerAlert[]>;

  isAlertmanagerConfigured = false;
  icons = Icons;
  showAlerts = false;
  simplebar = {
    autoHide: false
  };
  textClass: string;
  borderClass: string;
  alertType: string;
  alerts: AlertmanagerAlert[];
  crticialActiveAlerts: number;
  warningActiveAlerts: number;
  healthData: any;
  categoryPgAmount: Record<string, number> = {};
  totalPgs = 0;

  constructor(
    private summaryService: SummaryService,
    private configService: ConfigurationService,
    private mgrModuleService: MgrModuleService,
    private clusterService: ClusterService,
    private osdService: OsdService,
    private authStorageService: AuthStorageService,
    private featureToggles: FeatureTogglesService,
    private healthService: HealthService,
    public prometheusService: PrometheusService,
    private refreshIntervalService: RefreshIntervalService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    this.interval = this.refreshIntervalService.intervalData$.subscribe(() => {
      this.getHealth();
      this.triggerPrometheusAlerts();
      this.getCapacityCardData();
    });
    this.getDetailsCardData();
  }

  ngOnDestroy() {
    this.interval.unsubscribe();
  }

  getHealth() {
    this.healthService.getMinimalHealth().subscribe((data: any) => {
      this.healthData = data;
    });
  }

  toggleAlertsWindow(type: string) {
    type === 'danger' ? (this.alertType = 'critical') : (this.alertType = type);
    this.textClass = `text-${type}`;
    this.borderClass = `border-${type}`;
    this.showAlerts = !this.showAlerts;
  }

  getDetailsCardData() {
    this.configService.get('fsid').subscribe((data) => {
      this.detailsCardData.fsid = data['value'][0]['value'];
    });
    this.mgrModuleService.getConfig('orchestrator').subscribe((data) => {
      const orchStr = data['orchestrator'];
      this.detailsCardData.orchestrator = orchStr.charAt(0).toUpperCase() + orchStr.slice(1);
    });
    this.summaryService.subscribe((summary) => {
      const version = summary.version.replace('ceph version ', '').split(' ');
      this.detailsCardData.cephVersion =
        version[0] + ' ' + version.slice(2, version.length).join(' ');
    });
  }

  getCapacityCardData() {
    this.osdSettingsService = this.osdService
      .getOsdSettings()
      .pipe(take(1))
      .subscribe((data: any) => {
        this.osdSettings = data;
      });
    this.capacityService = this.clusterService.getCapacity().subscribe((data: any) => {
      this.capacity = data;
    });
  }

  triggerPrometheusAlerts() {
    this.prometheusService.ifAlertmanagerConfigured(() => {
      this.isAlertmanagerConfigured = true;

      this.prometheusService.getAlerts().subscribe((alerts) => {
        this.alerts = alerts;
        this.crticialActiveAlerts = alerts.filter(
          (alert: AlertmanagerAlert) =>
            alert.status.state === 'active' && alert.labels.severity === 'critical'
        ).length;
        this.warningActiveAlerts = alerts.filter(
          (alert: AlertmanagerAlert) =>
            alert.status.state === 'active' && alert.labels.severity === 'warning'
        ).length;
      });
    });
  }
}
