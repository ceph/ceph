import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { map } from 'rxjs/operators';
import { forkJoin, Observable } from 'rxjs';
import { BreakdownChartData, CapacityThreshold } from '../models/overview';

const StorageType = {
  BLOCK: $localize`Block`,
  FILE: $localize`File system`,
  OBJECT: $localize`Object`,
  SYSTEM_METADATA: $localize`System metadata`
} as const;

type StorageBreakdownMetric = {
  application: string;
  value: string;
};

type OverviewStorageInsights = {
  average_consumption_per_day: string | null;
  time_until_full_days: string | null;
  breakdown: StorageBreakdownMetric[];
  osd_full_ratio: string | null;
  osd_nearfull_ratio: string | null;
};

@Injectable({ providedIn: 'root' })
export class OverviewStorageService {
  private readonly http = inject(HttpClient);
  private readonly formatter = inject(FormatterService);
  private readonly baseUrl = 'api/prometheus/overview/storage';
  private readonly objectPoolsCountUrl = 'api/prometheus/prometheus_query_data';
  private readonly objectPoolsCountQuery = 'count(ceph_pool_metadata{application="Object"})';

  getTrendData(start: number, end: number, stepSec: number) {
    return this.http
      .get<{ total_raw_used: [number, string][] }>(`${this.baseUrl}/trend`, {
        params: {
          start,
          end,
          step: stepSec
        }
      })
      .pipe(
        map((result) => ({
          TOTAL_RAW_USED: (result?.total_raw_used ?? []).map((value) => [
            value[0],
            isNaN(parseFloat(value[1])) ? '0' : value[1]
          ]) as [number, string][]
        }))
      );
  }

  getAverageConsumption(): Observable<string> {
    return this.getStorageInsights().pipe(
      map((res) => {
        const v = Number(res?.average_consumption_per_day ?? 0);
        const [val, unit] = this.formatter.formatToBinary(v, true);
        return `${val} ${unit}/day`;
      })
    );
  }

  getTimeUntilFull(): Observable<string> {
    return this.getStorageInsights().pipe(
      map((res) => {
        const days = Number(res?.time_until_full_days ?? Infinity);
        if (!isFinite(days) || days <= 0) return 'N/A';

        if (days < 1) return `${(days * 24).toFixed(1)} hours`;
        if (days < 30) return `${days.toFixed(1)} days`;
        if (days < 365) return `${(days / 30).toFixed(1)} months`;
        return `${(days / 365).toFixed(1)} years`;
      })
    );
  }

  getTopPools(query: string): Observable<{ group: string; value: number }[]> {
    return this.http
      .get<{ result: Array<{ metric?: Record<string, string>; value?: [number, string] }> }>(
        this.objectPoolsCountUrl,
        { params: { params: query } }
      )
      .pipe(
        map((data) => {
          return (data?.result ?? []).map((r) => ({
            group: r.metric?.name || r.metric?.pool || 'unknown',
            value: Number(r.value?.[1]) * 100
          }));
        })
      );
  }

  getCount(query: string): Observable<number> {
    return this.http
      .get<{ result: Array<{ value?: [number, string] }> }>(this.objectPoolsCountUrl, {
        params: { params: query }
      })
      .pipe(map((res: any) => Number(res?.result?.[0]?.value?.[1]) || 0));
  }

  getObjectCounts(rgwBucketService: any) {
    return forkJoin({
      buckets: rgwBucketService.getTotalBucketsAndUsersLength(),
      pools: this.getCount(this.objectPoolsCountQuery)
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
    return this.getStorageInsights().pipe(
      map((data) => {
        return {
          osdFullRatio: this.formatGaugeMetric(data?.osd_full_ratio),
          osdNearfullRatio: this.formatGaugeMetric(data?.osd_nearfull_ratio)
        };
      })
    );
  }

  getStorageBreakdown(): Observable<StorageBreakdownMetric[]> {
    return this.getStorageInsights().pipe(map((data) => data?.breakdown ?? []));
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
    data: StorageBreakdownMetric[],
    unit: string,
    totalUsedBytes: number
  ): BreakdownChartData[] {
    if (!unit || totalUsedBytes == null || !data) return [];

    let assignedBytes = 0;

    const chartData = data.reduce<BreakdownChartData[]>((acc, r) => {
      const rawBytes = Number(r?.value ?? 0);
      if (!rawBytes) return acc;

      const group = this.normalizeGroup(r?.application);
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

  private getStorageInsights(): Observable<OverviewStorageInsights> {
    return this.http.get<OverviewStorageInsights>(this.baseUrl);
  }

  private formatGaugeMetric(data: string | null): number | null {
    const value = parseFloat(data ?? '');
    return isFinite(value) ? value : null;
  }
}
