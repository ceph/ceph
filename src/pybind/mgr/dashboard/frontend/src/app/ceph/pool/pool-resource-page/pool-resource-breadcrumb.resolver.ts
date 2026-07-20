import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class PoolResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const name = route.parent?.params?.name || route.params?.name || '';
    return [{ text: name, path: this.getFullPath(route) }];
  }
}
