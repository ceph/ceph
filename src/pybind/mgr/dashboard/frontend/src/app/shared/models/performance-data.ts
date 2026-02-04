import { ChartPoint } from './area-chart-point';

export interface PerformanceData {
  [PerformanceType.IOPS]: ChartPoint[];
  [PerformanceType.Latency]: ChartPoint[];
  [PerformanceType.Throughput]: ChartPoint[];
}

export interface TimeRange {
  start: number;
  end: number;
  step: number;
}

export interface StorageMetricData {
  readSpeed?: string;
  writeSpeed?: string;
  readIOPS?: string;
  writeIOPS?: string;
  getLatency?: string;
  putLatency?: string;
  replyLatency?: string;
  requests?: string;
  readThroughput?: string;
  writeThroughput?: string;
}

export interface ChartSeriesEntry {
  labels: { timestamp: string };
  value: string;
}

export interface StorageTypeMetrics {
  [StorageType.Filesystem]: StorageMetricData;
  [StorageType.Block]: StorageMetricData;
  [StorageType.Object]: StorageMetricData;
}

export enum StorageType {
  Filesystem = 'filesystem',
  Block = 'block',
  Object = 'object'
}

export enum PerformanceType {
  IOPS = 'iops',
  Latency = 'latency',
  Throughput = 'throughput'
}

export enum Units {
  IOPS = '',
  Latency = 'ms',
  Throughput = 'B/s'
}

export const METRIC_UNIT_MAP: Record<PerformanceType, string> = {
  [PerformanceType.Latency]: Units.Latency,
  [PerformanceType.Throughput]: Units.Throughput,
  [PerformanceType.IOPS]: Units.IOPS
};
