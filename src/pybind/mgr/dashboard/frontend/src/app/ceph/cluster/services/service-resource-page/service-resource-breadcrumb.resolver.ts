import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';
import { decodeServiceNameFromRoute } from '~/app/shared/models/service.interface';

@Injectable({
  providedIn: 'root'
})
export class ServiceResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const serviceName = decodeServiceNameFromRoute(route.paramMap.get('service_name') || '');

    return [
      { text: $localize`Administration/Services`, path: '/services' },
      { text: serviceName, path: this.getFullPath(route) }
    ];
  }
}
