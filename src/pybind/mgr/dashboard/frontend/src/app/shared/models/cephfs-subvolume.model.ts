export interface CephfsSubvolume {
  name: string;
  info: CephfsSubvolumeInfo;
}

export interface CephfsSubvolumeInfo {
  mode: number;
  type: string;
  bytes_pcent: number;
  bytes_quota: number;
  data_pool: string;
  path: string;
  state: string;
  created_at: string;
}
