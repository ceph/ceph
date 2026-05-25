import { inject, Injectable } from '@angular/core';
import { PrometheusService } from './prometheus.service';
import { PerformanceData } from '../models/performance-data';
import { AllStoragetypesQueries, NvmeofPromqls } from '../enum/dashboard-promqls.enum';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { ChartPoint } from '../models/area-chart-point';

export interface NvmeofThroughput {
  reads: number;
  writes: number;
  combined: number;
}

const BYTES_PER_MB = 1024 * 1024;

@Injectable({
  providedIn: 'root'
})
export class PerformanceCardService {
  private prometheusService = inject(PrometheusService);

  getNvmeofThroughput(
    time: { start: number; end: number; step: number } = this.prometheusService.lastHourDateObject
  ): Observable<NvmeofThroughput> {
    return this.prometheusService
      .getRangeQueriesData(time, NvmeofPromqls, true)
      .pipe(map((raw) => this.convertNvmeofThroughput(raw)));
  }

  convertNvmeofThroughput(raw: Record<string, [number, string][]>): NvmeofThroughput {
    const readValues = raw?.NVMEOF_READ_BYTES ?? [];
    const writeValues = raw?.NVMEOF_WRITE_BYTES ?? [];
    const combinedValues = raw?.NVMEOF_COMBINED_BYTES ?? [];
    const lastRead = readValues.length ? Number(readValues[readValues.length - 1][1]) : 0;
    const lastWrite = writeValues.length ? Number(writeValues[writeValues.length - 1][1]) : 0;
    const lastCombined = combinedValues.length
      ? Number(combinedValues[combinedValues.length - 1][1])
      : lastRead + lastWrite;
    return {
      reads: lastRead / BYTES_PER_MB,
      writes: lastWrite / BYTES_PER_MB,
      combined: lastCombined / BYTES_PER_MB
    };
  }

  getChartData(time: { start: number; end: number; step: number }): Observable<PerformanceData> {
    return this.prometheusService.getRangeQueriesData(time, AllStoragetypesQueries, true).pipe(
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
