export interface CephfsSubvolumeGroup {
  name: string;
  info?: CephfsSubvolumeGroupInfo;
}

export interface CephfsSubvolumeGroupInfo {
  mode: number;
  bytes_pcent: number;
  bytes_quota: number;
  data_pool: string;
  state: string;
  created_at: string;
}
