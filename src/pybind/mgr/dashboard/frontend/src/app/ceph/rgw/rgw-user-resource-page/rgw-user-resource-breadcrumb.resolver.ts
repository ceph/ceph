import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class RgwUserResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const uid = route.params?.uid || route.parent?.params?.uid || '';
    const section =
      route.firstChild?.url?.[0]?.path ||
      route.params?.section ||
      route.queryParams?.section ||
      'overview';
    const sectionLabel = section
      .split('-')
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
    return [
      { text: uid, path: `/rgw/user/${uid}/overview` },
      { text: sectionLabel, path: this.getFullPath(route) }
    ];
  }
}
