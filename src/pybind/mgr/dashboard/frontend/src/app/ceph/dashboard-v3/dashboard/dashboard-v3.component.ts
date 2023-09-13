import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { Observable, Subscription } from 'rxjs';
import { take } from 'rxjs/operators';
import moment from 'moment';

import { HealthService } from '~/app/shared/api/health.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { Promqls as queries } from '~/app/shared/enum/dashboard-promqls.enum';
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
import { PrometheusListHelper } from '~/app/shared/helpers/prometheus-list-helper';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';

@Component({
  selector: 'cd-dashboard-v3',
  templateUrl: './dashboard-v3.component.html',
  styleUrls: ['./dashboard-v3.component.scss']
})
export class DashboardV3Component extends PrometheusListHelper implements OnInit, OnDestroy {
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

  icons = Icons;
  showAlerts = false;
  flexHeight = true;
  simplebar = {
    autoHide: false
  };
  textClass: string;
  borderClass: string;
  alertType: string;
  alerts: AlertmanagerAlert[];
  healthData: any;
  categoryPgAmount: Record<string, number> = {};
  totalPgs = 0;
  queriesResults: any = {
    USEDCAPACITY: '',
    IPS: '',
    OPS: '',
    READLATENCY: '',
    WRITELATENCY: '',
    READCLIENTTHROUGHPUT: '',
    WRITECLIENTTHROUGHPUT: '',
    RECOVERYBYTES: ''
  };
  telemetryEnabled: boolean;
  telemetryURL = 'https://telemetry-public.ceph.com/';
  timerGetPrometheusDataSub: Subscription;
  timerTime = 30000;
  readonly lastHourDateObject = {
    start: moment().unix() - 3600,
    end: moment().unix(),
    step: 14
  };
  origin = window.location.origin;

  constructor(
    private summaryService: SummaryService,
    private orchestratorService: OrchestratorService,
    private osdService: OsdService,
    private authStorageService: AuthStorageService,
    private featureToggles: FeatureTogglesService,
    private healthService: HealthService,
    public prometheusService: PrometheusService,
    private mgrModuleService: MgrModuleService,
    private refreshIntervalService: RefreshIntervalService,
    public prometheusAlertService: PrometheusAlertService
  ) {
    super(prometheusService);
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    super.ngOnInit();
    this.interval = this.refreshIntervalService.intervalData$.subscribe(() => {
      this.getHealth();
      this.getCapacityCardData();
    });
    this.getPrometheusData(this.lastHourDateObject);
    this.getDetailsCardData();
    this.getTelemetryReport();
  }

  getTelemetryText(): string {
    return this.telemetryEnabled
      ? 'Cluster telemetry is active'
      : 'Cluster telemetry is inactive. To Activate the Telemetry, \
       click settings icon on top navigation bar and select \
       Telemetry configration.';
  }
  ngOnDestroy() {
    this.interval.unsubscribe();
    if (this.timerGetPrometheusDataSub) {
      this.timerGetPrometheusDataSub.unsubscribe();
    }
  }

  getHealth() {
    this.healthService.getMinimalHealth().subscribe((data: any) => {
      this.healthData = data;
    });
  }

  toggleAlertsWindow(type: string, isToggleButton: boolean = false) {
    this.triggerPrometheusAlerts();
    if (isToggleButton) {
      this.showAlerts = !this.showAlerts;
      this.flexHeight = !this.flexHeight;
    } else if (
      !this.showAlerts ||
      (this.alertType === type && type !== 'danger') ||
      (this.alertType !== 'warning' && type === 'danger')
    ) {
      this.showAlerts = !this.showAlerts;
      this.flexHeight = !this.flexHeight;
    }

    type === 'danger' ? (this.alertType = 'critical') : (this.alertType = type);
    this.textClass = `text-${type}`;
    this.borderClass = `border-${type}`;
  }

  getDetailsCardData() {
    this.healthService.getClusterFsid().subscribe((data: string) => {
      this.detailsCardData.fsid = data;
    });
    this.orchestratorService.getName().subscribe((data: string) => {
      this.detailsCardData.orchestrator = data;
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
    this.capacityService = this.healthService.getClusterCapacity().subscribe((data: any) => {
      this.capacity = data;
    });
  }

  triggerPrometheusAlerts() {
    this.prometheusService.ifAlertmanagerConfigured(() => {
      this.prometheusService.getAlerts().subscribe((alerts) => {
        this.alerts = alerts;
      });
    });
  }

  public getPrometheusData(selectedTime: any) {
    this.queriesResults = this.prometheusService.getPrometheusQueriesData(
      selectedTime,
      queries,
      this.queriesResults
    );
  }

  private getTelemetryReport() {
    this.mgrModuleService.getConfig('telemetry').subscribe((resp: any) => {
      this.telemetryEnabled = resp?.enabled;
    });
  }
}
