export enum HardwareNameMapping {
  memory = 'Memory',
  storage = 'Drive',
  processors = 'CPU',
  network = 'Network',
  power = 'Power supply',
  fans = 'Fan module',
  temperatures = 'Temperature'
}

export interface HardwareHealthCount {
  total: number;
  ok: number;
  warn: number;
  critical: number;
}

export interface HardwareSummary {
  total: {
    category: Record<string, HardwareHealthCount>;
    total: HardwareHealthCount;
  };
  host: Record<string, { flawed: boolean } | number>;
}

export interface CompressionStats {
  fcm_drive_count: number;
  phy_util_percent: number | null;
  log_util_percent: number | null;
  savings_bytes: number | null;
  compression_ratio: number | null;
  efficiency_percent: number | null;
}
