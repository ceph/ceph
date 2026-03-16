import { Injectable, inject } from '@angular/core';
import {
  PrometheusService,
  PromethuesGaugeMetricResult,
  PromqlGuageMetric
} from '~/app/shared/api/prometheus.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { map } from 'rxjs/operators';
import { forkJoin, Observable } from 'rxjs';

const StorageType = {
  BLOCK: $localize`Block`,
  FILE: $localize`File system`,
  OBJECT: $localize`Object`
} as const;

const CHART_GROUP_LABELS = new Set([StorageType.BLOCK, StorageType.FILE, StorageType.OBJECT]);

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

  convertBytesToUnit(value: string, unit: string): number {
    return this.formatter.convertToUnit(value, 'B', unit, 1);
  }

  getStorageBreakdown(): Observable<PromqlGuageMetric> {
    return this.prom.getPrometheusQueryData({ params: this.RAW_USED_BY_STORAGE_TYPE_QUERY });
  }
  formatBytesForChart(value: number): [number, string] {
    return this.formatter.formatToBinary(value, true);
  }

  mapStorageChartData(data: PromqlGuageMetric, unit: string): { group: string; value: number }[] {
    if (!unit) return [];

    const result = data?.result ?? [];

    return result
      .map((r: PromethuesGaugeMetricResult) => {
        const group = r?.metric?.application;
        const value = r?.value?.[1];

        return {
          group: group === 'Filesystem' ? StorageType.FILE : group,
          value: this.convertBytesToUnit(value, unit)
        };
      })
      .filter((item) => CHART_GROUP_LABELS.has(item.group) && item.value > 0);
  }
}
