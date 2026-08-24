import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { SmbService } from '~/app/shared/api/smb.service';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { Observable, Subscription, forkJoin, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';
import { PerformanceCardService } from '~/app/shared/api/performance-card.service';
import { PerformanceData, StorageType } from '~/app/shared/models/performance-data';
import { ChartPoint } from '~/app/shared/models/area-chart-point';

@Component({
  selector: 'cd-cephfs-overview',
  templateUrl: './cephfs-overview.component.html',
  styleUrls: ['./cephfs-overview.component.scss'],
  providers: [DimlessBinaryPipe],
  standalone: false
})
export class CephfsOverviewComponent implements OnInit, OnDestroy {
  isLoading = true;
  private subs = new Subscription();

  // KPIs
  fileSystemCount = 0;
  totalClientConnections = 0;
  activeMdsCount = 0;
  standbyMdsCount = 0;
  totalSubvolumes = 0;
  totalSubvolumeGroups = 0;

  // Health
  healthStatus = 'OK';
  healthColor: 'success' | 'warning' | 'danger' | 'info' = 'success';
  healthIssues: { type: string; name: string; state: string; color: string }[] = [];

  // Capacity
  totalCapacity = 0;
  totalUsed = 0;
  totalAvailable = 0;
  totalDataCapacity = 0;
  totalDataUsed = 0;
  totalMetadataCapacity = 0;
  totalMetadataUsed = 0;

  // Consumption Trend Data & KPIs
  consumptionTrendData: ChartPoint[] = [];
  averageDailyConsumption = '';
  estimatedTimeUntilFull = '';

  // Performance Charts Data
  iopsChartData: any[] = [];
  latencyChartData: any[] = [];
  throughputChartData: any[] = [];

  // Gateways & Protocols
  nfsClusterCount = 0;
  nfsExportCount = 0;
  smbClusterCount = 0;
  smbShareCount = 0;
  // Top File Systems
  topFileSystems: { name: string; dataUsed: number; available: number }[] = [];
  topSubvolumes: { name: string; fsName: string; bytes_used: number }[] = [];

  // Interactive Popover Breakdown State
  isTopConsumersOpen = false;
  selectedBreakdown = 'filesystem';
  breakdownOptions = [
    { label: 'By File System', value: 'filesystem' },
    { label: 'By Subvolume', value: 'subvolume' }
  ];

  // Mirroring
  mirrorDaemonCount = 0;
  mirrorPeerCount = 0;
  mirrorHealthStatus: string = 'OK';
  mirrorHealthColor: 'success' | 'warning' | 'danger' | 'info' = 'success';

  constructor(
    private cephfsService: CephfsService,
    private cephfsSubvolumeService: CephfsSubvolumeService,
    private cephfsSubvolumeGroupService: CephfsSubvolumeGroupService,
    private poolService: PoolService,
    private performanceCardService: PerformanceCardService,
    private prometheusService: PrometheusService,
    private formatterService: FormatterService,
    private nfsService: NfsService,
    private smbService: SmbService
  ) {}

  ngOnInit(): void {
    this.loadCephfsData();
    this.loadConsumptionTrend();
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  toggleTopConsumersPopover(): void {
    this.isTopConsumersOpen = !this.isTopConsumersOpen;
  }

  get popoverItems(): { name: string; subtext?: string; value: number }[] {
    if (this.selectedBreakdown === 'subvolume') {
      return this.topSubvolumes.map((sv) => ({
        name: sv.name,
        subtext: sv.fsName,
        value: sv.bytes_used || 0
      }));
    }
    return this.topFileSystems.map((fs) => ({
      name: fs.name,
      subtext: 'File System',
      value: fs.dataUsed
    }));
  }

  get popoverRoute(): string {
    return '/cephfs/fs';
  }

  loadConsumptionTrend(selectedTime?: { start: number; end: number; step: number }): void {
    const end = selectedTime?.end || Math.floor(Date.now() / 1000);
    const start = selectedTime?.start || end - 7 * 86400; // default 7 days
    const step = selectedTime?.step || 3600;

    const timeRange = { start, end, step };

    // 1. Fetch Consumption Trend Area Chart Points
    this.subs.add(
      this.prometheusService
        .getRangeQueriesData(
          timeRange,
          {
            CEPHFS_USED:
              'sum(ceph_pool_bytes_used * on(pool_id) group_left(application) ceph_pool_metadata{application=~"(.*Filesystem.*)|(.*cephfs.*)"})'
          },
          true
        )
        .pipe(catchError(() => of(null)))
        .subscribe((results: any) => {
          const rawTrend = results?.CEPHFS_USED || [];
          this.consumptionTrendData = rawTrend.map(([ts, val]: [number, string]) => ({
            timestamp: new Date(ts * 1000),
            values: { Used: Number(val) }
          }));
          if (!this.consumptionTrendData.length) {
            this.consumptionTrendData = [{ timestamp: new Date(), values: { Used: 0 } }];
          }
        })
    );

    const rateWindow = this.getRateWindow(start, end);

    // 2. Fetch Average Daily Consumption
    const avgQuery = `sum(rate(ceph_pool_bytes_used[${rateWindow}]) * on(pool_id) group_left(application) ceph_pool_metadata{application=~"(.*Filesystem.*)|(.*cephfs.*)"}) * 86400`;
    this.subs.add(
      this.prometheusService
        .getPrometheusQueryData({ params: avgQuery })
        .pipe(catchError(() => of(null)))
        .subscribe((res: any) => {
          const val = Number(res?.result?.[0]?.value?.[1] ?? 0);
          const [formattedVal, unit] = this.formatterService.formatToBinary(val, true);
          this.averageDailyConsumption = `${formattedVal} ${unit}/day`;
        })
    );

    // 3. Fetch Estimated Time Until Full
    const fullQuery = `(sum(ceph_pool_max_avail * on(pool_id) group_left(application) ceph_pool_metadata{application=~"(.*Filesystem.*)|(.*cephfs.*)"})) / (${avgQuery})`;
    this.subs.add(
      this.prometheusService
        .getPrometheusQueryData({ params: fullQuery })
        .pipe(catchError(() => of(null)))
        .subscribe((res: any) => {
          const days = Number(res?.result?.[0]?.value?.[1] ?? Infinity);
          this.estimatedTimeUntilFull = this.formatTimeUntilFull(days);
        })
    );
  }

  private getRateWindow(start: number, end: number): string {
    const diffSec = Math.max(60, end - start);
    if (diffSec <= 600) return '5m';
    if (diffSec <= 3600) return '1h';
    if (diffSec <= 86400) return '1d';
    if (diffSec <= 7 * 86400) return '7d';
    return '30d';
  }

  private formatTimeUntilFull(days: number): string {
    if (!isFinite(days) || days <= 0) return 'N/A';
    if (days < 1) return `${(days * 24).toFixed(1)} hours`;
    if (days < 30) return `${days.toFixed(1)} days`;
    if (days < 365) return `${(days / 30).toFixed(1)} months`;
    const years = days / 365;
    if (years > 10) return '> 10 years';
    return `${years.toFixed(1)} years`;
  }

  loadPerformanceData(selectedTime?: { start: number; end: number; step: number }): void {
    const time = selectedTime || {
      start: Math.floor(Date.now() / 1000) - 3600,
      end: Math.floor(Date.now() / 1000),
      step: 14
    };
    this.subs.add(
      this.performanceCardService
        .getChartData(time, StorageType.Filesystem)
        .pipe(catchError(() => of(null)))
        .subscribe((data: PerformanceData | null) => {
          if (data) {
            this.iopsChartData = data.iops || [];
            this.latencyChartData = data.latency || [];
            this.throughputChartData = data.throughput || [];
          }
        })
    );
  }

  private loadCephfsData() {
    this.isLoading = true;

    this.subs.add(
      forkJoin({
        cephfsList: this.cephfsService.list().pipe(catchError(() => of([]))),
        pools: this.poolService.getList().pipe(catchError(() => of([]))),
        nfsClusters: this.nfsService.nfsClusterList().pipe(catchError(() => of([]))),
        nfsExports: this.nfsService.list().pipe(catchError(() => of([]))),
        smbClusters: this.smbService.listClusters().pipe(catchError(() => of([]))),
        mirrorDaemons: this.cephfsService.listDaemonStatus().pipe(catchError(() => of([])))
      })
        .pipe(
          catchError(() =>
            of({
              cephfsList: [],
              pools: [],
              nfsClusters: [],
              nfsExports: [],
              smbClusters: [],
              mirrorDaemons: []
            })
          )
        )
        .subscribe((res: any) => {
          try {
            const cephfsList = Array.isArray(res?.cephfsList) ? res.cephfsList : [];
            const pools = Array.isArray(res?.pools) ? res.pools : [];
            const nfsClusters = Array.isArray(res?.nfsClusters) ? res.nfsClusters : [];
            const nfsExports = Array.isArray(res?.nfsExports) ? res.nfsExports : [];
            const smbClusters = Array.isArray(res?.smbClusters) ? res.smbClusters : [];
            const mirrorDaemons = Array.isArray(res?.mirrorDaemons) ? res.mirrorDaemons : [];

            this.processCephfsData(cephfsList, pools);

            this.nfsClusterCount = nfsClusters.length;
            this.nfsExportCount = nfsExports.length;
            this.smbClusterCount = smbClusters.length;

            this.mirrorDaemonCount = mirrorDaemons.length;
            let totalPeers = 0;
            let mirrorErrors = 0;
            mirrorDaemons.forEach((d: any) => {
              if (d && d.filesystems) {
                d.filesystems.forEach((fs: any) => {
                  if (fs.peers) {
                    totalPeers += fs.peers.length;
                    fs.peers.forEach((peer: any) => {
                      if (peer.stats && peer.stats.failure_count > 0) {
                        mirrorErrors++;
                      }
                    });
                  }
                });
              }
            });
            this.mirrorPeerCount = totalPeers;
            if (mirrorErrors > 0) {
              this.mirrorHealthStatus = `Error (${mirrorErrors})`;
              this.mirrorHealthColor = 'danger';
            } else {
              this.mirrorHealthStatus = 'OK';
              this.mirrorHealthColor = 'success';
            }

            const clientReqs: Observable<any>[] = [];
            const subvolumeReqs: Observable<any>[] = [];
            const subvolGroupReqs: Observable<any>[] = [];
            const smbShareReqs: Observable<any>[] = [];

            cephfsList.forEach((fs: any) => {
              if (!fs || !fs.id) return;
              clientReqs.push(
                this.cephfsService
                  .getClients(fs.id)
                  .pipe(catchError(() => of({ status: 1, data: [] })))
              );

              const fsName = fs.mdsmap?.fs_name || fs.name;
              if (fsName) {
                const groupsReq = this.cephfsSubvolumeGroupService
                  .get(fsName, false)
                  .pipe(catchError(() => of([])));
                subvolGroupReqs.push(groupsReq);

                const allSubvolsReq = groupsReq.pipe(
                  switchMap((groups) => {
                    const svReqs: Observable<any>[] = [];
                    svReqs.push(
                      this.cephfsSubvolumeService
                        .get(fsName, '', false)
                        .pipe(catchError(() => of([])))
                    );
                    if (Array.isArray(groups)) {
                      groups.forEach((g: any) => {
                        if (g && g.name && g.name !== '_nogroup') {
                          svReqs.push(
                            this.cephfsSubvolumeService
                              .get(fsName, g.name, false)
                              .pipe(catchError(() => of([])))
                          );
                        }
                      });
                    }
                    return forkJoin(svReqs);
                  }),
                  map((results) => {
                    let total = 0;
                    if (Array.isArray(results)) {
                      results.forEach((r: any) => {
                        if (Array.isArray(r)) total += r.length;
                      });
                    }
                    return total;
                  }),
                  catchError(() => of(0))
                );
                subvolumeReqs.push(allSubvolsReq);
              }
            });

            smbClusters.forEach((cluster: any) => {
              if (cluster && cluster.cluster_id) {
                smbShareReqs.push(
                  this.smbService.listShares(cluster.cluster_id).pipe(catchError(() => of([])))
                );
              }
            });

            if (cephfsList.length > 0 || smbClusters.length > 0) {
              forkJoin({
                clients: clientReqs.length ? forkJoin(clientReqs) : of([]),
                subvolumes: subvolumeReqs.length ? forkJoin(subvolumeReqs) : of([]),
                subvolGroups: subvolGroupReqs.length ? forkJoin(subvolGroupReqs) : of([]),
                smbShares: smbShareReqs.length ? forkJoin(smbShareReqs) : of([])
              })
                .pipe(
                  catchError(() =>
                    of({ clients: [], subvolumes: [], subvolGroups: [], smbShares: [] })
                  )
                )
                .subscribe({
                  next: ({ clients, subvolumes, subvolGroups, smbShares }) => {
                    let totalClients = 0;
                    if (Array.isArray(clients)) {
                      clients.forEach((res: any) => {
                        if (res && Array.isArray(res.data)) {
                          totalClients += res.data.length;
                        }
                      });
                    }
                    this.totalClientConnections = totalClients;

                    let totalSubvols = 0;
                    if (Array.isArray(subvolumes)) {
                      subvolumes.forEach((res: any) => {
                        if (typeof res === 'number') {
                          totalSubvols += res;
                        } else if (Array.isArray(res)) {
                          totalSubvols += res.length;
                        }
                      });
                    }
                    this.totalSubvolumes = totalSubvols;

                    let totalGroups = 0;
                    if (Array.isArray(subvolGroups)) {
                      subvolGroups.forEach((res: any) => {
                        if (Array.isArray(res)) {
                          totalGroups += res.length;
                        }
                      });
                    }
                    this.totalSubvolumeGroups = totalGroups;

                    let totalSmbShares = 0;
                    if (Array.isArray(smbShares)) {
                      smbShares.forEach((res: any) => {
                        if (Array.isArray(res)) {
                          totalSmbShares += res.length;
                        }
                      });
                    }
                    this.smbShareCount = totalSmbShares;
                    this.isLoading = false;
                  },
                  error: () => {
                    this.isLoading = false;
                  }
                });
            } else {
              this.isLoading = false;
            }
          } catch {
            this.isLoading = false;
          }
        })
    );
  }

  private processCephfsData(filesystems: any[], pools: any[]) {
    this.fileSystemCount = filesystems.length;

    let activeMds = 0;
    let standbyMds = 0;

    const dataPoolIds = new Set<number>();
    const metadataPoolIds = new Set<number>();

    let errorCount = 0;
    let warningCount = 0;

    // Build a map of filesystem name -> data pool IDs for Top 5 computation
    const fsDataPoolMap = new Map<string, Set<number>>();

    filesystems.forEach((fs) => {
      if (fs.mdsmap && fs.mdsmap.info) {
        Object.values(fs.mdsmap.info).forEach((daemon: any) => {
          if (daemon.state === 'up:active') activeMds++;
          else standbyMds++;
        });
      }

      const fsName = fs.mdsmap?.fs_name || fs.name || `fs-${fs.id}`;
      const fsPoolIds = new Set<number>();

      if (fs.mdsmap.data_pools) {
        fs.mdsmap.data_pools.forEach((pid: any) => {
          dataPoolIds.add(Number(pid));
          fsPoolIds.add(Number(pid));
        });
      }
      if (fs.mdsmap.metadata_pool) {
        metadataPoolIds.add(Number(fs.mdsmap.metadata_pool));
      }

      fsDataPoolMap.set(fsName, fsPoolIds);
    });

    this.activeMdsCount = activeMds;
    this.standbyMdsCount = standbyMds;

    let maxDataAvail = 0;

    // Calculate capacity
    pools.forEach((pool) => {
      const poolId = Number(pool.pool);
      const isData = dataPoolIds.has(poolId);
      const isMeta = metadataPoolIds.has(poolId);

      if (isData || isMeta) {
        const stats = pool.stats || {};
        const bytesUsed = stats.bytes_used?.latest || 0;
        const maxAvail = stats.max_avail?.latest || 0;

        if (isData) {
          this.totalDataUsed += bytesUsed;
          this.totalDataCapacity += bytesUsed + maxAvail;
          if (maxAvail > maxDataAvail) maxDataAvail = maxAvail;
        }
        if (isMeta) {
          this.totalMetadataUsed += bytesUsed;
          this.totalMetadataCapacity += bytesUsed + maxAvail;
        }

        this.totalUsed += bytesUsed;
      }
    });

    this.totalAvailable = maxDataAvail;
    this.totalCapacity = this.totalUsed + this.totalAvailable;

    // Compute Top 5 File Systems by data usage
    const fsUsageList: { name: string; dataUsed: number; available: number }[] = [];
    fsDataPoolMap.forEach((poolIds, fsName) => {
      let fsDataUsed = 0;
      let fsAvailable = 0;
      pools.forEach((pool) => {
        if (poolIds.has(Number(pool.pool))) {
          const stats = pool.stats || {};
          fsDataUsed += stats.bytes_used?.latest || 0;
          const avail = stats.max_avail?.latest || 0;
          if (avail > fsAvailable) fsAvailable = avail;
        }
      });
      fsUsageList.push({ name: fsName, dataUsed: fsDataUsed, available: fsAvailable });
    });
    this.topFileSystems = fsUsageList.sort((a, b) => b.dataUsed - a.dataUsed).slice(0, 5);

    if (errorCount > 0) {
      this.healthStatus = `Error (${errorCount})`;
      this.healthColor = 'danger';
    } else if (warningCount > 0) {
      this.healthStatus = `Warning (${warningCount})`;
      this.healthColor = 'warning';
    } else {
      this.healthStatus = 'OK';
      this.healthColor = 'success';
    }
  }

  get usagePercentNumber(): number {
    const logicalTotal = this.totalDataUsed + this.totalAvailable;
    if (logicalTotal > 0) {
      return (this.totalDataUsed / logicalTotal) * 100;
    }
    return 0;
  }

  get logicalTotalBytes(): number {
    return this.totalDataUsed + this.totalAvailable;
  }

  get overviewFields() {
    return [
      {
        label: $localize`File Systems`,
        value: this.fileSystemCount,
        type: 'text',
        routerLink: '/cephfs/fs'
      },
      { label: $localize`Subvolumes`, value: this.totalSubvolumes, type: 'text' },
      { label: $localize`Subvolume Groups`, value: this.totalSubvolumeGroups, type: 'text' },
      { label: $localize`Client Connections`, value: this.totalClientConnections, type: 'text' },
      { label: $localize`Active MDS Daemons`, value: this.activeMdsCount, type: 'text' },
      {
        label: $localize`Mirroring Health`,
        value: this.mirrorHealthStatus,
        type: 'status',
        status: this.mirrorHealthColor
      }
    ];
  }
}
