import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class OsdResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const osdId = route.parent?.params?.id || route.params?.id || '';
    return [
      { text: 'Cluster/OSDs', path: '/osd' },
      { text: osdId, path: this.getFullPath(route) }
    ];
  }
}
