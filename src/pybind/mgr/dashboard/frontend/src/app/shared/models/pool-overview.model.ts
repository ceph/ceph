import { ChartPoint } from '~/app/shared/models/area-chart-point';

export interface PoolOverviewModel {
  name: string;
  type: string;
  dataProtection: string;
  applications: string[];
  pgStatus: string;
  crushRuleset: string | number;
  usageTotal: number;
  usageUsed: number | null;
  usagePercent: string;
  usedCapacity: string;
  availableCapacity: string;
  totalCapacity: string;
  quotaLimit: string;
  isErasure: boolean;
  typeLabel: string;
  replicationSize: string;
  minSize: string;
  erasureK: string;
  erasureM: string;
  erasureTotal: string;
  erasurePlugin: string;
  failureDomain: string;
  readThroughput: string;
  readOps: string;
  readOpsChartData: ChartPoint[];
  writeThroughput: string;
  writeOps: string;
  writeOpsChartData: ChartPoint[];
}
