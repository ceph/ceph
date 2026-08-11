import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class SmbUsergroupsResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const standaloneName = decodeURIComponent(
      route.paramMap.get('users_groups_id') || route.parent?.paramMap.get('users_groups_id') || ''
    );

    return [{ text: standaloneName, path: this.getFullPath(route) }];
  }
}
