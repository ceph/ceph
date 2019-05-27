export class PoolStat {
  latest: number;
  rate: number;
  rates: number[];
}

export class PoolStats {
  bytes_used?: PoolStat;
  max_avail?: PoolStat;
  rd_bytes?: PoolStat;
  wr_bytes?: PoolStat;
  rd?: PoolStat;
  wr?: PoolStat;
}
