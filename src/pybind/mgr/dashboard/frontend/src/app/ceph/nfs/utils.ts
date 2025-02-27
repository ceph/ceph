import { SUPPORTED_FSAL } from './models/nfs.fsal';

export const getFsalFromRoute = (url: string): SUPPORTED_FSAL =>
  url.startsWith('/rgw/nfs') ? SUPPORTED_FSAL.RGW : SUPPORTED_FSAL.CEPH;

export const getPathfromFsal = (fsal: SUPPORTED_FSAL): string =>
  fsal === SUPPORTED_FSAL.CEPH ? 'cephfs' : 'rgw';
