import { ActivatedRoute } from '@angular/router';

import { SMB_BASE_CEPHFS, SMB_BASE_RGW } from '~/app/shared/constants/smb.constant';

import { resolveSmbRouteData } from './smb-route.util';

describe('resolveSmbRouteData', () => {
  const mockActivatedRoute = (snapshotDataList: Record<string, unknown>[]): ActivatedRoute => {
    return {
      pathFromRoot: snapshotDataList.map((data) => ({
        snapshot: { data }
      }))
    } as unknown as ActivatedRoute;
  };

  it('should return default values when no route in the tree has smbBasePath', () => {
    const route = mockActivatedRoute([{}, {}]);

    expect(resolveSmbRouteData(route)).toEqual({
      isRgw: false,
      smbBasePath: SMB_BASE_CEPHFS
    });
  });

  it('should resolve smbBasePath and isRgw from the route data when present', () => {
    const route = mockActivatedRoute([{}, { smbBasePath: SMB_BASE_RGW, isRgw: true }]);

    expect(resolveSmbRouteData(route)).toEqual({
      isRgw: true,
      smbBasePath: SMB_BASE_RGW
    });
  });

  it('should default isRgw to false when smbBasePath is present but isRgw is not', () => {
    const route = mockActivatedRoute([{ smbBasePath: SMB_BASE_CEPHFS }]);

    expect(resolveSmbRouteData(route)).toEqual({
      isRgw: false,
      smbBasePath: SMB_BASE_CEPHFS
    });
  });

  it('should use the first route in pathFromRoot that defines smbBasePath', () => {
    const route = mockActivatedRoute([
      { smbBasePath: SMB_BASE_CEPHFS, isRgw: false },
      { smbBasePath: SMB_BASE_RGW, isRgw: true }
    ]);

    expect(resolveSmbRouteData(route)).toEqual({
      isRgw: false,
      smbBasePath: SMB_BASE_CEPHFS
    });
  });

  it('should treat an empty string smbBasePath as a valid match', () => {
    const route = mockActivatedRoute([{ smbBasePath: '' }]);

    expect(resolveSmbRouteData(route)).toEqual({
      isRgw: false,
      smbBasePath: ''
    });
  });
});
