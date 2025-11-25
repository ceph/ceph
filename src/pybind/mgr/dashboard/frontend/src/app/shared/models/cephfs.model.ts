export interface MountData {
  clusterFSID: string;
  fsName: string;
  path: string;
}

export const PERMISSION_NAMES = {
  READ: 'read',
  WRITE: 'write',
  SNAPSHOT: 'snapshot',
  QUOTA: 'quota',
  ROOTSQUASH: 'rootSquash'
} as const;
