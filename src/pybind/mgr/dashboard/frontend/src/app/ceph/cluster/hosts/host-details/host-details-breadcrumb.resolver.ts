import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class HostDetailsBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const hostname = route.parent?.params?.hostname || route.params?.hostname || '';
    return [
      { text: 'Cluster/Hosts', path: '/hosts' },
      { text: hostname, path: this.getFullPath(route) }
    ];
  }
}
