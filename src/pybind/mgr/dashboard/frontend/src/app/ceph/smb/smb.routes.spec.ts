import { ModuleStatusGuardService } from '~/app/shared/services/module-status-guard.service';
import {
  SMB_CEPHFS_CONTEXT,
  SMB_CONTEXT,
  SMB_RGW_CONTEXT
} from '~/app/shared/constants/smb.constant';

import { smbChildRoutes, smbParentRoute } from './smb.routes';

describe('smbParentRoute', () => {
  it('should build the cephfs route shape when isRgw is false', () => {
    const route = smbParentRoute(false, 'File/SMB');

    expect(route.path).toBe('smb');
    expect(route.canActivate).toEqual([ModuleStatusGuardService]);
    expect(route.children).toBe(smbChildRoutes);
    expect(route.data).toEqual({
      breadcrumbs: 'File/SMB',
      moduleStatusGuardConfig: {
        uiApiPath: 'smb',
        redirectTo: 'error',
        header: 'SMB module is not enabled',
        module_name: 'smb',
        navigate_to: SMB_CEPHFS_CONTEXT.basePath
      }
    });
    expect(route.providers).toEqual([{ provide: SMB_CONTEXT, useValue: SMB_CEPHFS_CONTEXT }]);
  });

  it('should build the rgw route shape when isRgw is true', () => {
    const route = smbParentRoute(true, 'SMB');

    expect(route.path).toBe('smb');
    expect(route.canActivate).toEqual([ModuleStatusGuardService]);
    expect(route.children).toBe(smbChildRoutes);
    expect(route.data).toEqual({
      breadcrumbs: 'SMB',
      moduleStatusGuardConfig: {
        uiApiPath: 'smb',
        redirectTo: 'error',
        header: 'SMB module is not enabled',
        module_name: 'smb',
        navigate_to: SMB_RGW_CONTEXT.basePath
      }
    });
    expect(route.providers).toEqual([{ provide: SMB_CONTEXT, useValue: SMB_RGW_CONTEXT }]);
  });
});
