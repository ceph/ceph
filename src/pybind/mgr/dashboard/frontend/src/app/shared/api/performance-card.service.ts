import { inject, Injectable } from '@angular/core';
import { PrometheusService } from './prometheus.service';
import { PerformanceData, StorageType } from '../models/performance-data';
import {
  BlockStorageQueries,
  FilesystemStorageQueries,
  ObjectStorageQueries
} from '../enum/dashboard-promqls.enum';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { ChartPoint } from '../models/area-chart-point';

@Injectable({
  providedIn: 'root'
})
export class PerformanceCardService {
  private prometheusService = inject(PrometheusService);

  getChartData(
    time: { start: number; end: number; step: number },
    storageType: StorageType = StorageType.Block
  ): Observable<PerformanceData> {
    const queries =
      storageType === StorageType.Filesystem
        ? FilesystemStorageQueries
        : storageType === StorageType.Object
          ? ObjectStorageQueries
          : BlockStorageQueries;

    return this.prometheusService.getRangeQueriesData(time, queries, true).pipe(
      map((raw) => {
        const chartData = this.convertPerformanceData(raw);

        return {
          iops: chartData.iops.length
            ? chartData.iops
            : [{ timestamp: new Date(), values: { 'Read IOPS': 0, 'Write IOPS': 0 } }],

          latency: chartData.latency.length
            ? chartData.latency
            : [
                {
                  timestamp: new Date(),
                  values: { 'p99 Latency': 0, 'p95 Latency': 0, 'Median Latency': 0 }
                }
              ],

          throughput: chartData.throughput.length
            ? chartData.throughput
            : [{ timestamp: new Date(), values: { 'Read Throughput': 0, 'Write Throughput': 0 } }]
        };
      })
    );
  }

  convertPerformanceData(raw: any): PerformanceData {
    const hasPercentileLatency = raw?.LATENCYP99 || raw?.LATENCYP95 || raw?.LATENCYMEDIAN;
    const latencySeries = hasPercentileLatency
      ? this.mergeSeries(
          this.toSeries(raw?.LATENCYP99 || [], 'p99 Latency'),
          this.toSeries(raw?.LATENCYP95 || [], 'p95 Latency'),
          this.toSeries(raw?.LATENCYMEDIAN || [], 'Median Latency')
        )
      : this.mergeSeries(
          this.toSeries(raw?.READLATENCY || [], 'Read Latency'),
          this.toSeries(raw?.WRITELATENCY || [], 'Write Latency')
        );

    return {
      iops: this.mergeSeries(
        this.toSeries(raw?.READIOPS || [], 'Read IOPS'),
        this.toSeries(raw?.WRITEIOPS || [], 'Write IOPS')
      ),
      latency: latencySeries,
      throughput: this.mergeSeries(
        this.toSeries(raw?.READCLIENTTHROUGHPUT || [], 'Read Throughput'),
        this.toSeries(raw?.WRITECLIENTTHROUGHPUT || [], 'Write Throughput')
      )
    };
  }

  toSeries(metric: [number, string][], label: string): ChartPoint[] {
    return metric.map(([ts, val]) => ({
      timestamp: new Date(ts * 1000),
      values: { [label]: Number(val) }
    }));
  }

  mergeSeries(...series: ChartPoint[][]): ChartPoint[] {
    const map = new Map<number, ChartPoint>();

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

    return [...map.values()].sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
  }
}
