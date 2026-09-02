import { ActivatedRoute } from '@angular/router';

export const SMB_BASE_CEPHFS = 'cephfs/smb';
export const SMB_BASE_RGW = 'rgw/smb';

/**
 * Nested routes (e.g. cluster overview hosting the share list) do not always
 * inherit parent route data. Walk pathFromRoot to resolve SMB context.
 */
export function resolveSmbRouteData(route: ActivatedRoute): {
  isRgw: boolean;
  smbBasePath: string;
} {
  for (const r of route.pathFromRoot) {
    const smbBasePath = r.snapshot.data['smbBasePath'];
    if (smbBasePath != null) {
      return {
        isRgw: !!r.snapshot.data['isRgw'],
        smbBasePath
      };
    }
  }
  return { isRgw: false, smbBasePath: SMB_BASE_CEPHFS };
}
