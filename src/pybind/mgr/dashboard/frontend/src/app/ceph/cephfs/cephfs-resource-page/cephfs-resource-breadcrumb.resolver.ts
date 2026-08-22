import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class CephfsResourceBreadcrumbResolver extends BreadcrumbsResolver {
  constructor(private cephfsService: CephfsService) {
    super();
  }

  resolve(route: ActivatedRouteSnapshot) {
    const id = Number(route.paramMap.get('id') || route.params?.id || 0);
    const filesystemPath = `${this.getFullPath(route)}/overview`;

    if (!Number.isFinite(id) || id <= 0) {
      return of([
        { text: 'File/File Systems', path: '/cephfs/fs' },
        { text: route.params?.id || '', path: filesystemPath }
      ] as IBreadcrumb[]);
    }

    return this.cephfsService.getCephfs(id).pipe(
      map((filesystem: any) => {
        const filesystemName = filesystem?.cephfs?.name || filesystem?.mdsmap?.fs_name || `${id}`;

        return [
          { text: 'File/File Systems', path: '/cephfs/fs' },
          { text: filesystemName, path: filesystemPath }
        ] as IBreadcrumb[];
      }),
      catchError(() =>
        of([
          { text: 'File/File Systems', path: '/cephfs/fs' },
          { text: route.params?.id || `${id}`, path: filesystemPath }
        ] as IBreadcrumb[])
      )
    );
  }
}
