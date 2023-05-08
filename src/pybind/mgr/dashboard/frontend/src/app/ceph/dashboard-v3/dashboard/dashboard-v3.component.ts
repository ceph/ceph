import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { Observable, Subscription, timer } from 'rxjs';
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
  timerGetPrometheusDataSub: Subscription;
  timerTime = 30000;
  readonly lastHourDateObject = {
    start: moment().unix() - 3600,
    end: moment().unix(),
    step: 12
  };

  constructor(
    private summaryService: SummaryService,
    private orchestratorService: OrchestratorService,
    private osdService: OsdService,
    private authStorageService: AuthStorageService,
    private featureToggles: FeatureTogglesService,
    private healthService: HealthService,
    public prometheusService: PrometheusService,
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

  getPrometheusData(selectedTime: any) {
    this.prometheusService.ifPrometheusConfigured(() => {
      if (this.timerGetPrometheusDataSub) {
        this.timerGetPrometheusDataSub.unsubscribe();
      }
      this.timerGetPrometheusDataSub = timer(0, this.timerTime).subscribe(() => {
        selectedTime = this.updateTimeStamp(selectedTime);

        for (const queryName in queries) {
          if (queries.hasOwnProperty(queryName)) {
            const query = queries[queryName];
            let interval = selectedTime.step;

            if (query.includes('rate') && selectedTime.step < 20) {
              interval = 20;
            } else if (query.includes('rate')) {
              interval = selectedTime.step * 2;
            }

            const intervalAdjustedQuery = query.replace(/\[(.*?)\]/g, `[${interval}s]`);

            this.prometheusService
              .getPrometheusData({
                params: intervalAdjustedQuery,
                start: selectedTime['start'],
                end: selectedTime['end'],
                step: selectedTime['step']
              })
              .subscribe((data: any) => {
                if (data.result.length) {
                  this.queriesResults[queryName] = data.result[0].values;
                }
              });
          }
        }
      });
    });
  }

  private updateTimeStamp(selectedTime: any): any {
    let formattedDate = {};
    const date: number = selectedTime['start'] + this.timerTime / 1000;
    const dateNow: number = selectedTime['end'] + this.timerTime / 1000;
    formattedDate = {
      start: date,
      end: dateNow,
      step: selectedTime['step']
    };
    return formattedDate;
  }
}
