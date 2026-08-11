import { Component, OnDestroy, OnInit, Optional } from '@angular/core';

import _ from 'lodash';
import { Observable, ReplaySubject, Subject, Subscription, combineLatest, of } from 'rxjs';

import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { PoolService } from '~/app/shared/api/pool.service';

import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { ChartPoint } from '~/app/shared/models/area-chart-point';
import { catchError, shareReplay, switchMap, takeUntil, tap } from 'rxjs/operators';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { PerformanceCardService } from '~/app/shared/api/performance-card.service';
import { PerformanceData, StorageType } from '~/app/shared/models/performance-data';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';

@Component({
  selector: 'cd-rgw-overview-dashboard',
  templateUrl: './rgw-overview-dashboard.component.html',
  styleUrls: ['./rgw-overview-dashboard.component.scss'],
  standalone: false
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
  rawUsedBytes = 0;
  usableFreeBytes = 0;
  averageObjectSize = 0;
  topBuckets: any[] = [];
  topUsers: { owner: string; bucketCount: number; totalSize: number }[] = [];

  // Interactive Popover Breakdown State
  isTopConsumersOpen = false;
  selectedBreakdown = 'bucket';
  breakdownOptions = [
    { label: 'By Bucket', value: 'bucket' },
    { label: 'By User', value: 'user' }
  ];
  realmData: any;
  realmSub: Subscription;
  multisiteInfo: object[] = [];
  ZonegroupSub: Subscription;
  ZoneSUb: Subscription;
  queriesResults: Record<string, [number, string][]> = {
    RGW_REQUEST_PER_SECOND: [],
    BANDWIDTH: [],
    AVG_GET_LATENCY: [],
    AVG_PUT_LATENCY: []
  };
  requestsChartData: ChartPoint[] = [];
  latencyChartData: ChartPoint[] = [];
  bandwidthChartData: ChartPoint[] = [];
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
  fetchDataSub: Subscription;
  poolSub: Subscription;
  private destroy$ = new Subject<void>();

  constructor(
    private authStorageService: AuthStorageService,
    private refreshIntervalService: RefreshIntervalService,
    private rgwDaemonService: RgwDaemonService,
    private rgwRealmService: RgwRealmService,
    private rgwZonegroupService: RgwZonegroupService,
    private rgwZoneService: RgwZoneService,
    private rgwBucketService: RgwBucketService,
    private prometheusService: PrometheusService,
    private rgwMultisiteService: RgwMultisiteService,
    private notificationService: NotificationService,
    private performanceCardService: PerformanceCardService,
    @Optional() private poolService?: PoolService
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
      });
      this.getSyncStatus();
    });

    if (this.poolService) {
      this.poolSub = this.poolService
        .getList()
        .pipe(
          catchError(() => of([])),
          takeUntil(this.destroy$)
        )
        .subscribe((pools: any[]) => {
          const rgwPools = pools.filter(
            (p) => p.application_metadata && p.application_metadata.includes('rgw')
          );
          let avail = 0;
          let rawUsed = 0;
          const targetPools = rgwPools.length > 0 ? rgwPools : pools;
          targetPools.forEach((p) => {
            const poolAvail = p.stats?.max_avail?.latest || 0;
            if (poolAvail > avail) {
              avail = poolAvail;
            }
            rawUsed += p.stats?.bytes_used?.latest || 0;
          });
          this.rawUsedBytes = rawUsed;
          this.usableFreeBytes = avail;
        });
    }

    this.realmSub = this.rgwRealmService.list().subscribe((data: any) => {
      this.rgwRealmCount = data['realms'].length || 0;
    });
    this.ZonegroupSub = this.rgwZonegroupService.list().subscribe((data: any) => {
      this.rgwZonegroupCount = data['zonegroups'].length;
    });
    this.ZoneSUb = this.rgwZoneService.list().subscribe((data: any) => {
      this.rgwZoneCount = data['zones'].length;
    });
    this.getPrometheusData(this.prometheusService.lastHourDateObject);

    // Subscribe to buckets$ to get Top 5 Buckets and Top 5 Users by size
    this.rgwBucketService.buckets$?.pipe(takeUntil(this.destroy$)).subscribe((buckets: any[]) => {
      if (buckets && buckets.length > 0) {
        this.topBuckets = [...buckets]
          .sort((a, b) => (b.bucket_size || 0) - (a.bucket_size || 0))
          .slice(0, 5);

        // Group by owner for Top Users
        const userMap = new Map<
          string,
          { owner: string; bucketCount: number; totalSize: number }
        >();
        buckets.forEach((b: any) => {
          const owner = b.owner || 'Unknown';
          const size = b.bucket_size || 0;
          if (!userMap.has(owner)) {
            userMap.set(owner, { owner, bucketCount: 1, totalSize: size });
          } else {
            const existing = userMap.get(owner)!;
            existing.bucketCount += 1;
            existing.totalSize += size;
          }
        });
        this.topUsers = Array.from(userMap.values())
          .sort((a, b) => b.totalSize - a.totalSize)
          .slice(0, 5);
      }
    });
    this.multisiteSyncStatus$ = this.subject.pipe(
      switchMap(() =>
        this.rgwMultisiteService.getSyncStatus().pipe(
          tap((data: any) => {
            this.loading = false;
            this.replicaZonesInfo = data['dataSyncInfo'];
            this.metadataSyncInfo = data['metadataSyncInfo'];
            if (this.replicaZonesInfo.length === 0) {
              this.showMultisiteCard = false;
              this.loading = false;
            }
            [this.realm, this.zonegroup, this.zone] = data['primaryZoneData'];
          }),
          catchError((err) => {
            err.preventDefault();
            this.loading = false;
            this.showMultisiteCard = false;
            const errorMessage = $localize`Unable to fetch sync status`;
            this.notificationService.show(
              NotificationType.error,
              errorMessage,
              err.error.detail || err.error.message
            );
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
    this.poolSub?.unsubscribe();
    this.destroy$.next();
    this.destroy$.complete();
    this.prometheusService?.unsubscribe();
  }

  getPrometheusData(selectedTime: any) {
    this.performanceCardService
      .getChartData(selectedTime, StorageType.Object)
      .pipe(takeUntil(this.destroy$))
      .subscribe((data: PerformanceData) => {
        if (data) {
          this.requestsChartData = data.iops || [];
          this.latencyChartData = data.latency || [];
          this.bandwidthChartData = data.throughput || [];
        }
      });
  }

  toggleTopConsumersPopover(): void {
    this.isTopConsumersOpen = !this.isTopConsumersOpen;
  }

  get usagePercentNumber(): number {
    const logicalTotal = this.totalPoolUsedBytes + this.usableFreeBytes;
    if (logicalTotal > 0) {
      return (this.totalPoolUsedBytes / logicalTotal) * 100;
    }
    return 0;
  }

  get logicalTotalBytes(): number {
    return this.totalPoolUsedBytes + this.usableFreeBytes;
  }

  get popoverItems(): { name: string; subtext?: string; value: number }[] {
    if (this.selectedBreakdown === 'user') {
      return this.topUsers.map((u) => ({
        name: u.owner,
        subtext: `${u.bucketCount} buckets`,
        value: u.totalSize
      }));
    }
    return this.topBuckets.map((b) => ({
      name: b.bid,
      subtext: b.owner,
      value: b.bucket_size || 0
    }));
  }

  get popoverRoute(): string {
    return this.selectedBreakdown === 'user' ? '/rgw/user' : '/rgw/bucket';
  }

  get overviewFields(): OverviewField[] {
    return [
      {
        label: $localize`Gateway`,
        value: this.rgwDaemonCount,
        type: 'text',
        routerLink: '/rgw/daemon'
      },
      {
        label: $localize`Realm`,
        value: this.rgwRealmCount,
        type: 'text',
        routerLink: '/rgw/multisite'
      },
      {
        label: $localize`Zonegroup`,
        value: this.rgwZonegroupCount,
        type: 'text',
        routerLink: '/rgw/multisite'
      },
      {
        label: $localize`Zone`,
        value: this.rgwZoneCount,
        type: 'text',
        routerLink: '/rgw/multisite'
      },
      {
        label: $localize`Bucket`,
        value: this.rgwBucketCount,
        type: 'text',
        routerLink: '/rgw/bucket'
      },
      {
        label: $localize`User`,
        value: this.UserCount,
        type: 'text',
        routerLink: '/rgw/user'
      },
      {
        label: $localize`Objects`,
        value: this.objectCount,
        type: 'text'
      }
    ];
  }

  getSyncStatus() {
    this.subject.next();
  }

  trackByFn(zone: any) {
    return zone;
  }
}
