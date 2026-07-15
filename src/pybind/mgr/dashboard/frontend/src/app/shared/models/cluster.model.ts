export interface CephClusterUser {
  [key: string]: unknown;
}

export type CephAuthUser = {
  [key: string]: unknown;
  entity?: string;
  caps?: {
    mds?: string;
    mon?: string;
    osd?: string;
  };
};
