import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class SmbClusterResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const clusterName = this.getClusterNameFromRoute(route.paramMap.get('cluster_id') || '');

    return [{ text: clusterName, path: this.getFullPath(route) }];
  }

  private getClusterNameFromRoute(clusterIdRoute: string): string {
    if (!clusterIdRoute) {
      return '';
    }

    try {
      return decodeURIComponent(clusterIdRoute);
    } catch {
      return clusterIdRoute;
    }
  }
}
