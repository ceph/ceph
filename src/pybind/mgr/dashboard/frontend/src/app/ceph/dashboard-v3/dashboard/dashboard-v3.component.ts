import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { BehaviorSubject, EMPTY, Observable, Subject, Subscription, of } from 'rxjs';
import { catchError, exhaustMap, switchMap, takeUntil } from 'rxjs/operators';

import { HealthService } from '~/app/shared/api/health.service';
import { PrometheusService, PromqlGuageMetric } from '~/app/shared/api/prometheus.service';
import {
  CapacityCardQueries,
  UtilizationCardQueries
} from '~/app/shared/enum/dashboard-promqls.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import {
  CapacityCardDetails,
  DashboardDetails,
  InventoryCommonDetail,
  InventoryDetails
} from '~/app/shared/models/cd-details';
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
import {
  Health,
  HealthSnapshotMap,
  IscsiMap,
  PgStateCount
} from '~/app/shared/models/health.interface';
import { VERSION_PREFIX } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-dashboard-v3',
  templateUrl: './dashboard-v3.component.html',
  styleUrls: ['./dashboard-v3.component.scss']
})
export class DashboardV3Component extends PrometheusListHelper implements OnInit, OnDestroy {
  telemetryURL = 'https://telemetry-public.ceph.com/';
  origin = window.location.origin;
  icons = Icons;

  permissions: Permissions;

  hardwareSubject = new BehaviorSubject<any>([]);
  private subs = new Subscription();
  private destroy$ = new Subject<void>();

  enabledFeature$: FeatureTogglesMap$;
  prometheusAlerts$: Observable<AlertmanagerAlert[]>;
  isHardwareEnabled$: Observable<boolean>;
  hardwareSummary$: Observable<any>;
  managedByConfig$: Observable<any>;

  color: string;
  flexHeight = true;
  simplebar = {
    autoHide: true
  };
  borderClass: string;
  alertType: string;
  alertClass = AlertClass;

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
  detailsCardData: DashboardDetails = {};
  capacityCardData: CapacityCardDetails = {
    osdNearfull: null,
    osdFull: null
  };
  healthCardData: Health;
  hasHealthChecks: boolean;
  hardwareHealth: any;
  hardwareEnabled: boolean = false;
  hasHardwareError: boolean = false;
  totalCapacity: number = null;
  usedCapacity: number = null;
  hostsCount: number = null;
  monCount: number = null;
  poolCount: number = null;
  rgwCount: number = null;
  osdCount: { in: number; out: number; up: number; down: number } & InventoryCommonDetail = null;
  pgStatus: { statuses: PgStateCount[] } & InventoryCommonDetail = null;
  mgrStatus: InventoryDetails = null;
  mdsStatus: InventoryDetails = null;
  iscsiMap: IscsiMap = null;

  constructor(
    private summaryService: SummaryService,
    private orchestratorService: OrchestratorService,
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
    this.getPrometheusData(this.prometheusService.lastHourDateObject);
    this.getDetailsCardData();
    this.getTelemetryReport();
    this.getCapacityCardData();
    this.prometheusAlertService.getGroupedAlerts(true);
  }

  getTelemetryText(): string {
    return this.telemetryEnabled
      ? $localize`Cluster telemetry is active`
      : $localize`Cluster telemetry is inactive. To Activate the Telemetry, \
       click settings icon on top navigation bar and select \
       Telemetry configration.`;
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
        const version = summary.version.replace(`${VERSION_PREFIX} `, '').split(' ');
        this.detailsCardData.cephVersion =
          version[0] + ' ' + version.slice(2, version.length).join(' ');
      })
    );
  }

  public getPrometheusData(selectedTime: any) {
    this.queriesResults = this.prometheusService.getRangeQueriesData(
      selectedTime,
      UtilizationCardQueries,
      this.queriesResults
    );
  }

  getCapacityQueryValues(data: PromqlGuageMetric['result']) {
    let osdFull = null;
    let osdNearfull = null;
    if (data?.[0]?.metric?.['__name__'] === CapacityCardQueries.OSD_FULL) {
      osdFull = data[0]?.value?.[1];
      osdNearfull = data[1]?.value?.[1];
    } else {
      osdFull = data?.[1]?.value?.[1];
      osdNearfull = data?.[0]?.value?.[1];
    }
    return [osdFull, osdNearfull];
  }

  getCapacityCardData() {
    const CAPACITY_QUERY = `{__name__=~"${CapacityCardQueries.OSD_FULL}|${CapacityCardQueries.OSD_NEARFULL}"}`;
    this.prometheusService
      .getGaugeQueryData(CAPACITY_QUERY)
      .subscribe((data: PromqlGuageMetric) => {
        const [osdFull, osdNearfull] = this.getCapacityQueryValues(data?.result);
        this.capacityCardData.osdFull = this.prometheusService.formatGuageMetric(osdFull);
        this.capacityCardData.osdNearfull = this.prometheusService.formatGuageMetric(osdNearfull);
      });
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

  private safeSum(a: number, b: number): number | null {
    return a != null && b != null ? a + b : null;
  }

  private safeDifference(a: number, b: number): number | null {
    return a != null && b != null ? a - b : null;
  }

  loadInventories() {
    this.refreshIntervalObs(() => this.healthService.getHealthSnapshot()).subscribe({
      next: (data: HealthSnapshotMap) => {
        this.detailsCardData.fsid = data?.fsid;
        this.healthCardData = data?.health;
        this.hasHealthChecks = !!Object.keys(this.healthCardData?.checks ?? {})?.length;
        this.monCount = data?.monmap?.num_mons;

        const osdMap = data?.osdmap;
        const osdIn = osdMap?.in;
        const osdUp = osdMap?.up;
        const osdTotal = osdMap?.num_osds;

        this.osdCount = {
          in: osdIn,
          up: osdUp,
          total: osdTotal,
          down: this.safeDifference(osdTotal, osdUp),
          out: this.safeDifference(osdTotal, osdIn)
        };

        const pgmap = data?.pgmap;
        this.poolCount = pgmap?.num_pools;
        this.usedCapacity = pgmap?.bytes_used;
        this.totalCapacity = pgmap?.bytes_total;
        this.pgStatus = {
          statuses: pgmap?.pgs_by_state,
          total: pgmap?.num_pgs
        };

        const mgrmap = data?.mgrmap;
        const mgrInfo = mgrmap?.num_standbys;
        const mgrSuccess = mgrmap?.num_active;

        this.mgrStatus = {
          info: mgrInfo,
          success: mgrSuccess,
          total: this.safeSum(mgrInfo, mgrSuccess)
        };

        const mdsInfo = data?.fsmap?.num_standbys;
        const mdsSuccess = data?.fsmap?.num_active;

        this.mdsStatus = {
          info: mdsInfo,
          success: mdsSuccess,
          total: this.safeSum(mdsInfo, mdsSuccess)
        };

        this.rgwCount = data?.num_rgw_gateways;
        this.iscsiMap = data?.num_iscsi_gateways;
        this.hostsCount = data?.num_hosts;
        this.enabledFeature$ = this.featureToggles.get();
      }
    });
  }
}
