import { inject, Injectable } from '@angular/core';
import { PrometheusService } from './prometheus.service';
import { PerformanceData, StorageType } from '../models/performance-data';
import { AllStoragetypesQueries } from '../enum/dashboard-promqls.enum';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class PerformanceCardService {
  private prometheusService = inject(PrometheusService);

  getChartData(
    time: { start: number; end: number; step: number },
    selectedStorageType: StorageType
  ): Observable<PerformanceData> {
    const queries = this.buildQueriesForStorageType(selectedStorageType);

    return this.prometheusService.getRangeQueriesData(time, queries, true).pipe(
      map((raw) => {
        const chartData = this.convertPerformanceData(raw);

        return {
          iops: chartData.iops.length
            ? chartData.iops
            : [{ timestamp: new Date(), values: { 'Read IOPS': 0, 'Write IOPS': 0 } }],

          latency: chartData.latency.length
            ? chartData.latency
            : [{ timestamp: new Date(), values: { 'Read Latency': 0, 'Write Latency': 0 } }],

          throughput: chartData.throughput.length
            ? chartData.throughput
            : [{ timestamp: new Date(), values: { 'Read Throughput': 0, 'Write Throughput': 0 } }]
        };
      })
    );
  }

  private buildQueriesForStorageType(storageType: StorageType) {
    const queries: any = {};

    const applicationFilter =
      storageType === StorageType.All ? '' : `{application="${storageType}"}`;

    Object.entries(AllStoragetypesQueries).forEach(([key, query]) => {
      queries[key] = query.replace('{{applicationFilter}}', applicationFilter);
    });

    return queries;
  }

  convertPerformanceData(raw: any): PerformanceData {
    return {
      iops: this.mergeSeries(
        this.toSeries(raw?.READIOPS || [], 'Read IOPS'),
        this.toSeries(raw?.WRITEIOPS || [], 'Write IOPS')
      ),
      latency: this.mergeSeries(
        this.toSeries(raw?.READLATENCY || [], 'Read Latency'),
        this.toSeries(raw?.WRITELATENCY || [], 'Write Latency')
      ),
      throughput: this.mergeSeries(
        this.toSeries(raw?.READCLIENTTHROUGHPUT || [], 'Read Throughput'),
        this.toSeries(raw?.WRITECLIENTTHROUGHPUT || [], 'Write Throughput')
      )
    };
  }

  private toSeries(metric: [number, string][], label: string) {
    return metric.map(([ts, val]) => ({
      timestamp: new Date(ts * 1000),
      values: { [label]: Number(val) }
    }));
  }

  private mergeSeries(...series: any[]) {
    const map = new Map<number, any>();

    for (const items of series) {
      for (const item of items) {
        const time = item.timestamp.getTime();

        if (!map.has(time)) {
          map.set(time, {
            timestamp: item.timestamp,
            values: { ...item.values }
          });
        } else {
          Object.assign(map.get(time).values, item.values);
        }
      }
    }

    return [...map.values()].sort((a, b) => a.timestamp - b.timestamp);
  }
}
