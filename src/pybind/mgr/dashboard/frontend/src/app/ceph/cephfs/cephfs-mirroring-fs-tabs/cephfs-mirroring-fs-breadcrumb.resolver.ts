import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { CEPHFS_MIRRORING_PAGE_HEADER } from '~/app/shared/constants/app.constants';
import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class CephfsMirroringFsBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const fsName = route.params?.fsName || '';
    let decodedFsName = fsName;
    try {
      decodedFsName = decodeURIComponent(fsName);
    } catch {
      // Fallback to raw value if decoding fails
    }
    return [
      { text: CEPHFS_MIRRORING_PAGE_HEADER.title, path: '/cephfs/mirroring' },
      { text: decodedFsName, path: null }
    ];
  }
}
