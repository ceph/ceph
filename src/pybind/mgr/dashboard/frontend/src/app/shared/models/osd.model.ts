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

interface Tree {
  device_class: string;
}

interface Host {
  id: number;
  name: string;
}

interface StatsHistory {
  out_bytes: any[];
  in_bytes: any[];
  op_out_bytes: any[];
  op_in_bytes: any[];
}

interface Stats {
  stat_bytes_used: number;
  stat_bytes: number;
  op_w: number;
  op_r: number;
  usage: number;
}
