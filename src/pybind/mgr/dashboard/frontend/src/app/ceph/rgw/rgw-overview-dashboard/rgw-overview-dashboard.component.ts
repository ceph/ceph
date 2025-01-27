import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { Observable, ReplaySubject, Subscription, combineLatest, of } from 'rxjs';

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
  realmSub: Subscription;
  multisiteInfo: object[] = [];
  ZonegroupSub: Subscription;
  ZoneSUb: Subscription;
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
  fetchDataSub: Subscription;

  constructor(
    private authStorageService: AuthStorageService,
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
      this.fetchDataSub = combineLatest([
        this.rgwDaemonService.list(),
        this.rgwBucketService.fetchAndTransformBuckets(),
        this.rgwBucketService.totalNumObjects$,
        this.rgwBucketService.totalUsedCapacity$,
        this.rgwBucketService.averageObjectSize$,
        this.rgwBucketService.getTotalBucketsAndUsersLength()
      ]).subscribe(([daemonData, _, objectCount, usedCapacity, averageSize, bucketData]) => {
        this.rgwDaemonCount = daemonData.length;
        this.objectCount = objectCount;
        this.totalPoolUsedBytes = usedCapacity;
        this.averageObjectSize = averageSize;
        this.rgwBucketCount = bucketData.buckets_count;
        this.UserCount = bucketData.users_count;
        this.getSyncStatus();
      });
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
    this.interval?.unsubscribe();
    this.realmSub?.unsubscribe();
    this.ZonegroupSub?.unsubscribe();
    this.ZoneSUb?.unsubscribe();
    this.fetchDataSub?.unsubscribe();
    this.prometheusService?.unsubscribe();
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
