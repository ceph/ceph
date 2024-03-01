import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { Observable, ReplaySubject, Subscription, of } from 'rxjs';

import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';

import { RgwPromqls as queries } from '~/app/shared/enum/dashboard-promqls.enum';
import { HealthService } from '~/app/shared/api/health.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { catchError, shareReplay, switchMap, tap } from 'rxjs/operators';

@Component({
  selector: 'cd-rgw-overview-dashboard',
  templateUrl: './rgw-overview-dashboard.component.html',
  styleUrls: ['./rgw-overview-dashboard.component.scss']
})
export class RgwOverviewDashboardComponent implements OnInit, OnDestroy {
  icons = Icons;

  interval = new Subscription();
  permissions: Permissions;
  rgwDaemonCount = 0;
  rgwRealmCount = 0;
  rgwZonegroupCount = 0;
  rgwZoneCount = 0;
  rgwBucketCount = 0;
  objectCount = 0;
  UserCount = 0;
  totalPoolUsedBytes = 0;
  averageObjectSize = 0;
  realmData: any;
  daemonSub: Subscription;
  realmSub: Subscription;
  multisiteInfo: object[] = [];
  ZonegroupSub: Subscription;
  ZoneSUb: Subscription;
  HealthSub: Subscription;
  BucketSub: Subscription;
  queriesResults: { [key: string]: [] } = {
    RGW_REQUEST_PER_SECOND: [],
    BANDWIDTH: [],
    AVG_GET_LATENCY: [],
    AVG_PUT_LATENCY: []
  };
  timerGetPrometheusDataSub: Subscription;
  chartTitles = ['Metadata Sync', 'Data Sync'];
  realm: string;
  zonegroup: string;
  zone: string;
  metadataSyncInfo: string;
  replicaZonesInfo: any = [];
  metadataSyncData: {};
  showMultisiteCard = true;
  loading = true;
  multisiteSyncStatus$: Observable<any>;
  subject = new ReplaySubject<any>();
  syncCardLoading = true;

  constructor(
    private authStorageService: AuthStorageService,
    private healthService: HealthService,
    private refreshIntervalService: RefreshIntervalService,
    private rgwDaemonService: RgwDaemonService,
    private rgwRealmService: RgwRealmService,
    private rgwZonegroupService: RgwZonegroupService,
    private rgwZoneService: RgwZoneService,
    private rgwBucketService: RgwBucketService,
    private prometheusService: PrometheusService,
    private rgwMultisiteService: RgwMultisiteService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.interval = this.refreshIntervalService.intervalData$.subscribe(() => {
      this.daemonSub = this.rgwDaemonService.list().subscribe((data: any) => {
        this.rgwDaemonCount = data.length;
      });
      this.HealthSub = this.healthService.getClusterCapacity().subscribe((data: any) => {
        this.objectCount = data['total_objects'];
        this.totalPoolUsedBytes = data['total_pool_bytes_used'];
        this.averageObjectSize = data['average_object_size'];
      });
      this.getSyncStatus();
    });
    this.BucketSub = this.rgwBucketService
      .getTotalBucketsAndUsersLength()
      .subscribe((data: any) => {
        this.rgwBucketCount = data['buckets_count'];
        this.UserCount = data['users_count'];
      });
    this.realmSub = this.rgwRealmService.list().subscribe((data: any) => {
      this.rgwRealmCount = data['realms'].length;
    });
    this.ZonegroupSub = this.rgwZonegroupService.list().subscribe((data: any) => {
      this.rgwZonegroupCount = data['zonegroups'].length;
    });
    this.ZoneSUb = this.rgwZoneService.list().subscribe((data: any) => {
      this.rgwZoneCount = data['zones'].length;
    });
    this.getPrometheusData(this.prometheusService.lastHourDateObject);
    this.multisiteSyncStatus$ = this.subject.pipe(
      switchMap(() =>
        this.rgwMultisiteService.getSyncStatus().pipe(
          tap((data: any) => {
            this.loading = false;
            this.replicaZonesInfo = data['dataSyncInfo'];
            this.metadataSyncInfo = data['metadataSyncInfo'];
            if (this.replicaZonesInfo.length === 0) {
              this.showMultisiteCard = false;
              this.syncCardLoading = false;
              this.loading = false;
            }
            [this.realm, this.zonegroup, this.zone] = data['primaryZoneData'];
          }),
          catchError((err) => {
            this.showMultisiteCard = false;
            this.syncCardLoading = false;
            this.loading = false;
            err.preventDefault();
            return of(true);
          })
        )
      ),
      shareReplay(1)
    );
  }

  ngOnDestroy() {
    this.interval.unsubscribe();
    this.daemonSub.unsubscribe();
    this.realmSub.unsubscribe();
    this.ZonegroupSub.unsubscribe();
    this.ZoneSUb.unsubscribe();
    this.BucketSub.unsubscribe();
    this.HealthSub.unsubscribe();
    this.prometheusService.unsubscribe();
  }

  getPrometheusData(selectedTime: any) {
    this.queriesResults = this.prometheusService.getPrometheusQueriesData(
      selectedTime,
      queries,
      this.queriesResults,
      true
    );
  }

  getSyncStatus() {
    this.subject.next();
  }

  trackByFn(zone: any) {
    return zone;
  }
}
