import { Component, OnDestroy, OnInit } from '@angular/core';
import { Subscription, forkJoin, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';
import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { PerformanceCardService } from '~/app/shared/api/performance-card.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { PerformanceData } from '~/app/shared/models/performance-data';
import { ChartPoint } from '~/app/shared/models/area-chart-point';
import { IscsiService } from '~/app/shared/api/iscsi.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';

@Component({
  selector: 'cd-rbd-overview',
  templateUrl: './rbd-overview.component.html',
  styleUrls: ['./rbd-overview.component.scss'],
  standalone: false
})
export class RbdOverviewComponent implements OnInit, OnDestroy {
  icons = Icons;

  // Mirroring / Health State
  healthStatus: string = 'OK';
  healthColor: 'success' | 'warning' | 'danger' | 'info-circle' = 'success';
  daemonCount = 0;
  isMirroringPanelOpen = false;
  mirroringIssues: { type: string; name: string; state: string; color: string }[] = [];
  activeBlockAlerts: any[] = [];

  // Capacity metrics
  rawUsedBytes = 0;
  logicalUsedBytes = 0;
  usableFreeBytes = 0;
  rawTotalBytes = 0;
  logicalTotalBytes = 0;

  // Consumption Trend Data & KPIs
  consumptionTrendData: ChartPoint[] = [];
  averageDailyConsumption = '';
  estimatedTimeUntilFull = '';

  // KPI Counts
  totalPools = 0;
  totalImages = 0;
  totalSnapshots = 0;
  totalNamespaces = 0;
  totalPeers = 0;

  // Performance Statistics Area Charts (Matching Object Overview)
  iopsChartData: ChartPoint[] = [];
  latencyChartData: ChartPoint[] = [];
  throughputChartData: ChartPoint[] = [];

  // Top Images data model
  topImages: any[] = [];
  topPools: any[] = [];

  // Interactive Popover Breakdown State
  isTopConsumersOpen = false;
  selectedBreakdown = 'image';
  breakdownOptions = [
    { label: 'By Image', value: 'image' },
    { label: 'By Pool', value: 'pool' }
  ];

  // Gateways & Protocols
  iscsiTargetCount = 0;
  nvmeofSubsystemCount = 0;
  nvmeofGatewayCount = 0;

  // Efficiency & Trash
  trashImageCount = 0;
  reclaimableSpace = 0;

  isLoading = true;

  private subs = new Subscription();

  constructor(
    private rbdMirroringService: RbdMirroringService,
    private rbdService: RbdService,
    private poolService: PoolService,
    private performanceCardService: PerformanceCardService,
    private iscsiService: IscsiService,
    private nvmeofService: NvmeofService,
    private prometheusAlertService: PrometheusAlertService,
    private prometheusService: PrometheusService,
    private formatterService: FormatterService
  ) {}

  ngOnInit(): void {
    this.loadMirroringSummary();
    this.loadBlockOverviewData();
    this.loadConsumptionTrend();
    this.loadPerformanceData();
    this.loadProtocolAndEfficiencyData();
    this.loadActiveBlockAlerts();
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  toggleMirroringPanel(): void {
    this.isMirroringPanelOpen = !this.isMirroringPanelOpen;
  }

  toggleTopConsumersPopover(): void {
    this.isTopConsumersOpen = !this.isTopConsumersOpen;
  }

  get popoverItems(): { name: string; subtext?: string; value: number }[] {
    if (this.selectedBreakdown === 'pool') {
      return this.topPools.map((p) => ({
        name: p.pool_name,
        subtext: 'Pool',
        value: p.stats?.bytes_used?.latest || 0
      }));
    }
    return this.topImages.map((img) => ({
      name: img.name,
      subtext: img.pool_name,
      value: img.disk_usage || img.size || 0
    }));
  }

  get popoverRoute(): string {
    return this.selectedBreakdown === 'pool' ? '/pool' : '/block/rbd';
  }

  private loadProtocolAndEfficiencyData(): void {
    // iSCSI Targets
    this.subs.add(
      this.iscsiService
        .listTargets()
        .pipe(catchError(() => of([])))
        .subscribe((targets: any[]) => {
          this.iscsiTargetCount = targets.length;
        })
    );

    // NVMe-oF Gateways and Subsystems
    this.subs.add(
      this.nvmeofService
        .listGatewayGroups()
        .pipe(
          switchMap((gatewayGroups: any) => {
            const groups = gatewayGroups?.[0] ?? [];
            if (groups.length === 0) {
              return of({ gatewayCount: 0, subsystemCount: 0 });
            }

            const subsystemRequests = groups.map((group: any) => {
              const isRunning = (group.status?.running ?? 0) > 0;
              const groupGatewayCount = group.status?.size ?? 0;
              if (!isRunning) {
                return of({ groupGatewayCount, subsystemCount: 0 });
              }
              return this.nvmeofService.listSubsystems(group.spec.group).pipe(
                map((subs: any) => ({
                  groupGatewayCount,
                  subsystemCount: Array.isArray(subs) ? subs.length : 0
                })),
                catchError(() => of({ groupGatewayCount, subsystemCount: 0 }))
              );
            });

            return forkJoin(subsystemRequests).pipe(
              map((results: any[]) => {
                let gatewayCount = 0;
                let subsystemCount = 0;
                results.forEach((res) => {
                  gatewayCount += res.groupGatewayCount;
                  subsystemCount += res.subsystemCount;
                });
                return { gatewayCount, subsystemCount };
              })
            );
          }),
          catchError(() => of({ gatewayCount: 0, subsystemCount: 0 }))
        )
        .subscribe(({ gatewayCount, subsystemCount }) => {
          this.nvmeofGatewayCount = gatewayCount;
          this.nvmeofSubsystemCount = subsystemCount;
        })
    );

    // RBD Trash
    this.subs.add(
      this.rbdService
        .listTrash()
        .pipe(catchError(() => of([])))
        .subscribe((trashPools: any[]) => {
          let trashCount = 0;
          let reclaimable = 0;
          trashPools.forEach((pool: any) => {
            const images = pool.value || [];
            trashCount += images.length;
            images.forEach((img: any) => {
              reclaimable += img.size || 0;
            });
          });
          this.trashImageCount = trashCount;
          this.reclaimableSpace = reclaimable;
        })
    );
  }

  get usagePercentNumber(): number {
    const logicalTotal = this.logicalUsedBytes + this.usableFreeBytes;
    if (logicalTotal > 0) {
      return (this.logicalUsedBytes / logicalTotal) * 100;
    }
    return 0;
  }

  get overviewFields(): OverviewField[] {
    return [
      { label: $localize`RBD Pools`, value: this.totalPools, type: 'text', routerLink: '/pool' },
      {
        label: $localize`Total Images`,
        value: this.totalImages,
        type: 'text',
        routerLink: '/block/rbd'
      },
      {
        label: $localize`Namespaces`,
        value: this.totalNamespaces,
        type: 'text',
        routerLink: '/block/rbd/namespaces'
      },
      {
        label: $localize`Snapshots`,
        value: this.totalSnapshots,
        type: 'text',
        routerLink: '/block/rbd'
      },
      {
        label: $localize`Trash Images`,
        value: this.trashImageCount,
        type: 'text',
        routerLink: '/block/rbd/trash'
      },
      {
        label: $localize`Mirroring Health`,
        value: this.healthStatus,
        type: 'status',
        status: this.healthColor,
        action: () => this.toggleMirroringPanel()
      }
    ];
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
            BLOCK_USED:
              'sum(ceph_pool_bytes_used * on(pool_id) group_left(application) ceph_pool_metadata{application=~"(.*Block.*)|(.*rbd.*)"})'
          },
          true
        )
        .pipe(catchError(() => of(null)))
        .subscribe((results: any) => {
          const rawTrend = results?.BLOCK_USED || [];
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
    const avgQuery = `sum(rate(ceph_pool_bytes_used[${rateWindow}]) * on(pool_id) group_left(application) ceph_pool_metadata{application=~"(.*Block.*)|(.*rbd.*)"}) * 86400`;
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
    const fullQuery = `(sum(ceph_pool_max_avail * on(pool_id) group_left(application) ceph_pool_metadata{application=~"(.*Block.*)|(.*rbd.*)"})) / (${avgQuery})`;
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
        .getChartData(time)
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

  private loadMirroringSummary(): void {
    this.subs.add(this.rbdMirroringService.startPolling());
    this.subs.add(
      this.rbdMirroringService.subscribeSummary((summary: any) => {
        if (!summary) return;
        let mirroringErrors = 0;
        let mirroringWarnings = 0;

        this.mirroringIssues = [];

        const daemons = summary.content_data?.daemons || [];
        this.daemonCount = daemons.length;

        // Helper to extract callout messages from daemon status
        const getCallouts = (poolName?: string, forDaemon?: any) => {
          let msgs: string[] = [];
          const sourceDaemons = forDaemon ? [forDaemon] : daemons;
          sourceDaemons.forEach((d: any) => {
            if (d.status) {
              Object.entries(d.status).forEach(([pName, pData]: [string, any]) => {
                if ((!poolName || pName === poolName) && pData.callouts) {
                  Object.values(pData.callouts).forEach((callout: any) => {
                    if (callout.level === 'error' || callout.level === 'warning') {
                      msgs.push(callout.text || 'Unknown issue');
                    }
                  });
                }
              });
            }
          });
          return Array.from(new Set(msgs)).join(', ');
        };

        daemons.forEach((d: any) => {
          if (d.health_color === 'error' || d.health_color === 'warning') {
            if (d.health_color === 'error') mirroringErrors++;
            else mirroringWarnings++;
            let stateStr = d.health || 'Unknown State';
            const callouts = getCallouts(undefined, d);
            if (callouts) {
              stateStr += ` - ${callouts}`;
            }
            this.mirroringIssues.push({
              type: 'Daemon',
              name: d.server_hostname || d.id || 'Unknown',
              state: stateStr,
              color: d.health_color
            });
          }
        });

        const pools = summary.content_data?.pools || [];
        pools.forEach((p: any) => {
          if (p.health_color === 'error' || p.health_color === 'warning') {
            if (p.health_color === 'error') mirroringErrors++;
            else mirroringWarnings++;
            let stateStr = p.health || 'Unknown State';
            const callouts = getCallouts(p.name);
            if (callouts) {
              stateStr += ` - ${callouts}`;
            }
            this.mirroringIssues.push({
              type: 'Pool',
              name: p.name || 'Unknown',
              state: stateStr,
              color: p.health_color
            });
          }
        });

        const imageErrors = summary.content_data?.image_error || [];
        imageErrors.forEach((i: any) => {
          if (i.state_color === 'error' || i.state_color === 'warning') {
            if (i.state_color === 'error') mirroringErrors++;
            else mirroringWarnings++;
            let stateStr = i.state || 'Error';
            if (i.description) {
              stateStr += ` - ${i.description}`;
            }
            this.mirroringIssues.push({
              type: 'Image',
              name: (i.pool_name ? i.pool_name + '/' : '') + (i.name || 'Unknown'),
              state: stateStr,
              color: i.state_color
            });
          }
        });

        if (mirroringErrors > 0) {
          this.healthStatus = `Error (${mirroringErrors})`;
          this.healthColor = 'danger';
        } else if (mirroringWarnings > 0) {
          this.healthStatus = `Warning (${mirroringWarnings})`;
          this.healthColor = 'warning';
        } else {
          this.healthStatus = 'OK';
          this.healthColor = 'success';
        }

        this.totalPeers = pools.reduce(
          (acc: number, p: any) => acc + (p.peer_uuids ? p.peer_uuids.length : 0),
          0
        );
      })
    );
  }

  private loadBlockOverviewData(): void {
    this.isLoading = true;
    forkJoin({
      poolsData: this.poolService.getList().pipe(
        catchError(() => of([])),
        switchMap((pools: any[]) => {
          const rbdPools = pools.filter(
            (p) => p.application_metadata && p.application_metadata.includes('rbd')
          );
          if (rbdPools.length === 0) {
            return of({ pools, namespacesCount: 0 });
          }
          const nsRequests = rbdPools.map((p) =>
            this.rbdService.listNamespaces(p.pool_name).pipe(catchError(() => of([])))
          );
          return forkJoin(nsRequests).pipe(
            map((namespacesArrays: any[]) => {
              const namespacesCount = namespacesArrays.reduce(
                (acc, nsArr) => acc + nsArr.length,
                0
              );
              return { pools, namespacesCount };
            })
          );
        })
      ),
      images: this.rbdService.list({}).pipe(catchError(() => of([])))
    }).subscribe(({ poolsData, images }) => {
      this.isLoading = false;
      const pools = poolsData.pools;
      this.totalNamespaces = poolsData.namespacesCount;

      // Filter RBD pools
      const rbdPools = (pools as any[]).filter(
        (p) => p.application_metadata && p.application_metadata.includes('rbd')
      );
      this.totalPools = rbdPools.length;

      let avail = 0;
      let rawUsed = 0;
      rbdPools.forEach((p) => {
        const poolAvail = p.stats?.max_avail?.latest || 0;
        if (poolAvail > avail) {
          avail = poolAvail; // Take the maximum available space across pools to avoid double counting shared OSDs
        }
        rawUsed += p.stats?.bytes_used?.latest || 0;
      });

      // Flatten RBD images across pools
      const allImages: any[] = [];
      let totalSnaps = 0;
      let used = 0;

      (images as any[]).forEach((poolData: any) => {
        const poolName = poolData.pool_name;
        const poolImages = poolData.value || [];
        poolImages.forEach((img: any) => {
          used += img.disk_usage || 0;
          totalSnaps += img.snapshots ? img.snapshots.length : 0;
          allImages.push({
            ...img,
            pool_name: poolName
          });
        });
      });

      this.rawUsedBytes = rawUsed;
      this.logicalUsedBytes = used;
      this.usableFreeBytes = avail;
      this.rawTotalBytes = rawUsed + avail;
      this.logicalTotalBytes = used + avail;

      this.totalImages = allImages.length;
      this.totalSnapshots = totalSnaps;

      // Sorted list of images for tests
      this.topImages = allImages.sort((a, b) => (b.size || 0) - (a.size || 0)).slice(0, 5);

      // Sorted list of pools for popover
      this.topPools = [...rbdPools]
        .sort((a, b) => (b.stats?.bytes_used?.latest || 0) - (a.stats?.bytes_used?.latest || 0))
        .slice(0, 5);
    });
  }

  private loadActiveBlockAlerts(): void {
    this.prometheusAlertService.getGroupedAlerts(true);
    this.subs.add(
      this.prometheusAlertService.alerts$.subscribe((alerts) => {
        this.activeBlockAlerts = (alerts || []).filter((alert) => {
          if (alert.status?.state !== 'active') return false;
          const name = (alert.labels?.alertname || '').toLowerCase();
          return (
            name.includes('rbd') ||
            name.includes('nvme') ||
            name.includes('iscsi') ||
            name.includes('tcmu') ||
            name.includes('osd')
          );
        });
      })
    );
  }
}
