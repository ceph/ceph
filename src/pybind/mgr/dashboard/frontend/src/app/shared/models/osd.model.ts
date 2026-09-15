/* We will need to check what are all the value that the
   UI need and only make them the mandatory parameters here.
   For now based on what I saw in the unit test file;
   osd-list.component.spec.ts, I've made the decision to make
   things optional and non-optional. This should be re-evaluated. */

import { ChartPoint } from './area-chart-point';

export type OsdHistoryRatePoint = [number, number] | number;

export interface OsdDetails {
  osd_map: Record<string, unknown>;
  osd_metadata: Record<string, unknown>;
  smart: Record<string, unknown>;
}

export interface Osd {
  id: number;
  host: Host;
  stats_history: StatsHistory;
  state: string[];
  stats: Stats;
  collectedStates?: string[];
  in?: number;
  out?: number;
  up?: number;
  down?: number;
  destroyed?: number;
  cdIsBinary?: boolean;
  cdIndivFlags?: string[];
  cdClusterFlags?: string[];
  cdExecuting?: any;
  tree?: Tree;
  operational_status?: string;
}

export interface OsdIoOverviewModel {
  readBytes: string;
  writeBytes: string;
  readOps: string;
  writeOps: string;
  readBytesChartData: ChartPoint[];
  writeBytesChartData: ChartPoint[];
}

export interface OsdCapacityOverviewModel {
  name: string;
  usageTotal: number;
  usageUsed: number | null;
  usagePercent: string;
  usedCapacity: string;
  availableCapacity: string;
  totalCapacity: string;
}

interface Tree {
  device_class: string;
}

interface Host {
  id: number;
  name: string;
}

interface StatsHistory {
  op_out_bytes: OsdHistoryRatePoint[];
  op_in_bytes: OsdHistoryRatePoint[];
  out_bytes?: number[];
  in_bytes?: number[];
}

interface Stats {
  stat_bytes_used: number;
  stat_bytes: number;
  numpg?: number;
  op_w?: number;
  op_r?: number;
  usage?: number;
}
