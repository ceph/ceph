import { Injectable, inject } from '@angular/core';
import {
  PrometheusService,
  PromethuesGaugeMetricResult,
  PromqlGuageMetric
} from '~/app/shared/api/prometheus.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { map } from 'rxjs/operators';
import { forkJoin, Observable } from 'rxjs';
import { CapacityCardQueries } from '../enum/dashboard-promqls.enum';
import { BreakdownChartData, CapacityThreshold } from '../models/overview';

const StorageType = {
  BLOCK: $localize`Block`,
  FILE: $localize`File system`,
  OBJECT: $localize`Object`,
  SYSTEM_METADATA: $localize`System metadata`
} as const;

@Injectable({ providedIn: 'root' })
export class OverviewStorageService {
  private readonly prom = inject(PrometheusService);
  private readonly formatter = inject(FormatterService);
  private readonly AVG_CONSUMPTION_QUERY = 'sum(rate(ceph_osd_stat_bytes_used[7d])) * 86400';
  private readonly TIME_UNTIL_FULL_QUERY = `(sum(ceph_osd_stat_bytes)) / (sum(rate(ceph_osd_stat_bytes_used[7d])) * 86400)`;
  private readonly TOTAL_RAW_USED_QUERY = 'sum(ceph_osd_stat_bytes_used)';
  private readonly OBJECT_POOLS_COUNT_QUERY = 'count(ceph_pool_metadata{application="Object"})';
  private readonly RAW_USED_BY_STORAGE_TYPE_QUERY =
    'sum by (application) (ceph_pool_bytes_used * on(pool_id) group_left(instance, name, application) ceph_pool_metadata{application=~"(.*Block.*)|(.*Filesystem.*)|(.*Object.*)|(..*)"})';
  private readonly FULL_NEARFULL_QUERY = `{__name__=~"${CapacityCardQueries.OSD_FULL}|${CapacityCardQueries.OSD_NEARFULL}"}`;

  getTrendData(start: number, end: number, stepSec: number) {
    const range = {
      start,
      end,
      step: stepSec
    };

    return this.prom.getRangeQueriesData(
      range,
      {
        TOTAL_RAW_USED: this.TOTAL_RAW_USED_QUERY
      },
      true
    );
  }

  getAverageConsumption(): Observable<string> {
    return this.prom.getPrometheusQueryData({ params: this.AVG_CONSUMPTION_QUERY }).pipe(
      map((res) => {
        const v = Number(res?.result?.[0]?.value?.[1] ?? 0);
        const [val, unit] = this.formatter.formatToBinary(v, true);
        return `${val} ${unit}/day`;
      })
    );
  }

  getTimeUntilFull(): Observable<string> {
    return this.prom.getPrometheusQueryData({ params: this.TIME_UNTIL_FULL_QUERY }).pipe(
      map((res) => {
        const days = Number(res?.result?.[0]?.value?.[1] ?? Infinity);
        if (!isFinite(days) || days <= 0) return 'N/A';

        if (days < 1) return `${(days * 24).toFixed(1)} hours`;
        if (days < 30) return `${days.toFixed(1)} days`;
        if (days < 365) return `${(days / 30).toFixed(1)} months`;
        return `${(days / 365).toFixed(1)} years`;
      })
    );
  }

  getTopPools(query: string): Observable<{ group: string; value: number }[]> {
    return this.prom.getPrometheusQueryData({ params: query }).pipe(
      map((data: PromqlGuageMetric) => {
        return (data?.result ?? []).map((r) => ({
          group: r.metric?.name || r.metric?.pool || 'unknown',
          value: Number(r.value?.[1]) * 100
        }));
      })
    );
  }

  getCount(query: string): Observable<number> {
    return this.prom
      .getPrometheusQueryData({ params: query })
      .pipe(map((res: any) => Number(res?.result?.[0]?.value?.[1]) || 0));
  }

  getObjectCounts(rgwBucketService: any) {
    return forkJoin({
      buckets: rgwBucketService.getTotalBucketsAndUsersLength(),
      pools: this.getCount(this.OBJECT_POOLS_COUNT_QUERY)
    }).pipe(
      map(({ buckets, pools }: { buckets: { buckets_count: number }; pools: number }) => ({
        buckets: buckets?.buckets_count ?? 0,
        pools
      }))
    );
  }

  getRawCapacityThresholds(): Observable<{
    osdFullRatio: number | null;
    osdNearfullRatio: number | null;
  }> {
    return this.prom.getGaugeQueryData(this.FULL_NEARFULL_QUERY).pipe(
      map((data: PromqlGuageMetric) => {
        const result = data?.result ?? [];

        const osdFull = result.find((r) => r.metric?.__name__ === CapacityCardQueries.OSD_FULL)
          ?.value?.[1];
        const osdNearfull = result.find(
          (r) => r.metric?.__name__ === CapacityCardQueries.OSD_NEARFULL
        )?.value?.[1];

        return {
          osdFullRatio: this.prom.formatGuageMetric(osdFull),
          osdNearfullRatio: this.prom.formatGuageMetric(osdNearfull)
        };
      })
    );
  }

  getStorageBreakdown(): Observable<PromqlGuageMetric> {
    return this.prom.getPrometheusQueryData({ params: this.RAW_USED_BY_STORAGE_TYPE_QUERY });
  }

  getThresholdStatus(total, used, nearfull, full): CapacityThreshold {
    if (!used || !total || !nearfull || !full) {
      return null;
    }

    const usageRatio = used / total;

    if (usageRatio >= full) return 'critical';
    else if (usageRatio >= nearfull) return 'high';

    return null;
  }

  convertBytesToUnit(value: number, unit: string): number {
    return this.formatter.convertToUnit(value, 'B', unit, 1);
  }

  formatBytesForChart(value: number): [number, string] {
    return this.formatter.formatToBinary(value, true);
  }

  private normalizeGroup(group: string): string {
    if (group === 'Filesystem') return StorageType.FILE;
    return group;
  }

  private isAStorage(group: string): boolean {
    return (
      group === StorageType.BLOCK || group === StorageType.FILE || group === StorageType.OBJECT
    );
  }

  mapStorageChartData(
    data: PromqlGuageMetric,
    unit: string,
    totalUsedBytes: number
  ): BreakdownChartData[] {
    if (!unit || totalUsedBytes == null || !data) return [];

    let assignedBytes = 0;

    const result: PromethuesGaugeMetricResult[] = data.result ?? [];
    const chartData = result.reduce<BreakdownChartData[]>((acc, r) => {
      const rawBytes = Number(r?.value?.[1] ?? 0);
      if (!rawBytes) return acc;

      const group = this.normalizeGroup(r?.metric?.application);
      const value = this.convertBytesToUnit(rawBytes, unit);

      if (this.isAStorage(group) && value > 0) {
        assignedBytes += rawBytes;
        acc.push({ group, value });
      }

      return acc;
    }, []);

    const systemBytes = Math.max(0, totalUsedBytes - assignedBytes);

    if (systemBytes > 0) {
      chartData.push({
        group: StorageType.SYSTEM_METADATA,
        value: this.convertBytesToUnit(systemBytes, unit)
      });
    }

    return chartData;
  }
}
