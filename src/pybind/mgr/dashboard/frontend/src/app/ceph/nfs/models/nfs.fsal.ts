export const RGW_USER_EXPORT_PATH = '/';

export enum SUPPORTED_FSAL {
  CEPH = 'CEPH',
  RGW = 'RGW'
}
export interface NfsFSAbstractionLayer {
  value: SUPPORTED_FSAL;
  descr: string;
  disabled: boolean;
}
