import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { BehaviorSubject, EMPTY, Observable, Subject, Subscription, of } from 'rxjs';
import { catchError, exhaustMap, switchMap, take, takeUntil } from 'rxjs/operators';

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
import { AlertClass } from '~/app/shared/enum/health-icon.enum';
import { HardwareService } from '~/app/shared/api/hardware.service';
import { SettingsService } from '~/app/shared/api/settings.service';
import { OsdSettings } from '~/app/shared/models/osd-settings';
import { IscsiMap } from '~/app/shared/models/health.interface';

@Component({
  selector: 'cd-dashboard-v3',
  templateUrl: './dashboard-v3.component.html',
  styleUrls: ['./dashboard-v3.component.scss']
})
export class DashboardV3Component extends PrometheusListHelper implements OnInit, OnDestroy {
  detailsCardData: DashboardDetails = {};
  osdSettingsService: any;
  osdSettings = new OsdSettings();
  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;
  color: string;
  capacityService: any;
  healthData$: Observable<Object>;
  prometheusAlerts$: Observable<AlertmanagerAlert[]>;

  icons = Icons;
  flexHeight = true;
  simplebar = {
    autoHide: true
  };
  borderClass: string;
  alertType: string;
  alertClass = AlertClass;
  healthData: any;
  categoryPgAmount: Record<string, number> = {};
  totalPgs = 0;
  queriesResults: { [key: string]: [] } = {
    USEDCAPACITY: [],
    IPS: [],
    OPS: [],
    READLATENCY: [],
    WRITELATENCY: [],
    READCLIENTTHROUGHPUT: [],
    WRITECLIENTTHROUGHPUT: [],
    RECOVERYBYTES: [],
    READIOPS: [],
    WRITEIOPS: []
  };
  telemetryEnabled: boolean;
  telemetryURL = 'https://telemetry-public.ceph.com/';
  origin = window.location.origin;
  hardwareHealth: any;
  hardwareEnabled: boolean = false;
  hasHardwareError: boolean = false;
  isHardwareEnabled$: Observable<boolean>;
  hardwareSummary$: Observable<any>;
  hardwareSubject = new BehaviorSubject<any>([]);
  managedByConfig$: Observable<any>;
  private subs = new Subscription();
  private destroy$ = new Subject<void>();

  hostsCount: number = null;
  monCount: number = null;
  poolCount: number = null;
  osdCount: any = null;
  totalCapacity: number = null;
  usedCapacity: number = null;
  poolStatus: Record<string, any>[] = null;
  pgStatus: any = null;
  mgrStatus: any = null;
  mdsStatus: any = null;
  rgwCount: number = null;
  iscsiMap: IscsiMap = null;

  constructor(
    private summaryService: SummaryService,
    private orchestratorService: OrchestratorService,
    private osdService: OsdService,
    private authStorageService: AuthStorageService,
    private featureToggles: FeatureTogglesService,
    private healthService: HealthService,
    private settingsService: SettingsService,
    public prometheusService: PrometheusService,
    private mgrModuleService: MgrModuleService,
    private refreshIntervalService: RefreshIntervalService,
    public prometheusAlertService: PrometheusAlertService,
    private hardwareService: HardwareService
  ) {
    super(prometheusService);
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    super.ngOnInit();
    if (this.permissions.configOpt.read) {
      this.getOsdSettings();
      this.isHardwareEnabled$ = this.getHardwareConfig();
      this.hardwareSummary$ = this.hardwareSubject.pipe(
        switchMap(() =>
          this.hardwareService.getSummary().pipe(
            switchMap((data: any) => {
              this.hasHardwareError = data.host.flawed;
              return of(data);
            })
          )
        )
      );
      this.managedByConfig$ = this.settingsService.getValues('MANAGED_BY_CLUSTERS');
    }

    this.loadInventories();
    this.getStatusData();
    this.getPrometheusData(this.prometheusService.lastHourDateObject);
    this.getDetailsCardData();
    this.getTelemetryReport();
    this.prometheusAlertService.getAlerts(true);
  }

  getStatusData() {
    this.healthService.getStatus().subscribe((data: any) => {
      this.detailsCardData.fsid = data.fsid;
      this.healthData = data.health;
      this.monCount = data?.monmap?.num_mons;
      const osdMap = data.osdmap;
      this.osdCount = {
        in: osdMap.num_in_osds,
        up: osdMap.num_up_osds,
        down: osdMap.num_osds - osdMap.num_up_osds,
        out: osdMap.num_osds - osdMap.num_in_osds,
        total: osdMap.num_osds
      };
      const pgmap = data.pgmap;
      this.poolCount = pgmap.num_pools;
      this.usedCapacity = pgmap.bytes_used;
      this.totalCapacity = pgmap.bytes_total;
      this.pgStatus = {
        statuses: pgmap.pgs_by_state,
        total: pgmap.num_pgs
      };
      const mgrmap = data?.mgrmap;
      const activeCount = mgrmap.available ? 1 : 0;
      this.mgrStatus = {
        info: mgrmap.num_standbys,
        success: activeCount,
        total: activeCount + mgrmap.num_standbys
      };
      this.mdsStatus = {
        standbys: data.fsmap?.['up:standby'],
        mdsmap: data.fsmap.by_rank
      };
    });
  }

  getTelemetryText(): string {
    return this.telemetryEnabled
      ? 'Cluster telemetry is active'
      : 'Cluster telemetry is inactive. To Activate the Telemetry, \
       click settings icon on top navigation bar and select \
       Telemetry configration.';
  }
  ngOnDestroy() {
    this.prometheusService.unsubscribe();
    this.subs?.unsubscribe();
    this.destroy$.next();
    this.destroy$.complete();
  }

  toggleAlertsWindow(type: AlertClass) {
    this.alertType === type ? (this.alertType = null) : (this.alertType = type);
  }

  getDetailsCardData() {
    this.orchestratorService.getName().subscribe((data: string) => {
      this.detailsCardData.orchestrator = data;
    });
    this.subs.add(
      this.summaryService.subscribe((summary) => {
        const version = summary.version.replace('ceph version ', '').split(' ');
        this.detailsCardData.cephVersion =
          version[0] + ' ' + version.slice(2, version.length).join(' ');
      })
    );
  }

  private getOsdSettings() {
    this.osdSettingsService = this.osdService
      .getOsdSettings()
      .pipe(take(1))
      .subscribe((data: OsdSettings) => {
        this.osdSettings = data;
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
    this.healthService.getTelemetryStatus().subscribe((enabled: boolean) => {
      this.telemetryEnabled = enabled;
    });
  }

  trackByFn(index: any) {
    return index;
  }

  getHardwareConfig(): Observable<any> {
    return this.mgrModuleService.getConfig('cephadm').pipe(
      switchMap((resp: any) => {
        this.hardwareEnabled = resp?.hw_monitoring;
        return of(resp?.hw_monitoring);
      })
    );
  }

  refreshIntervalObs(fn: Function) {
    return this.refreshIntervalService.intervalData$.pipe(
      exhaustMap(() => fn().pipe(catchError(() => EMPTY))),
      takeUntil(this.destroy$)
    );
  }

  loadInventories() {
    this.refreshIntervalObs(() => this.healthService.getMinimalHealth()).subscribe({
      next: (result: any) => {
        this.hostsCount = result.hosts;
        this.rgwCount = result.rgw;
        this.iscsiMap = result.iscsi_daemons;
        this.enabledFeature$ = this.featureToggles.get();
      }
    });
  }
}
