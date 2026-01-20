import { Injectable, inject } from '@angular/core';
import { PrometheusService, PromqlGuageMetric } from '~/app/shared/api/prometheus.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { map } from 'rxjs/operators';
import { forkJoin, Observable } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class OverviewStorageService {
  private readonly prom = inject(PrometheusService);
  private readonly formatter = inject(FormatterService);
  private readonly AVG_CONSUMPTION_QUERY = 'sum(rate(ceph_pool_bytes_used[7d])) * 86400';
  private readonly TIME_UNTIL_FULL_QUERY = `(sum(ceph_pool_max_avail)) / (sum(rate(ceph_pool_bytes_used[7d])) * 86400)`;
  private readonly TOTAL_RAW_USED_QUERY = 'sum(ceph_pool_bytes_used)';
  private readonly OBJECT_POOLS_COUNT_QUERY = 'count(ceph_pool_metadata{application="Object"})';

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
        if (!isFinite(days) || days <= 0) return '∞';

        if (days < 1) return `${(days * 24).toFixed(1)} hours`;
        if (days < 30) return `${days.toFixed(1)} days`;
        return `${(days / 30).toFixed(1)} months`;
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
}
