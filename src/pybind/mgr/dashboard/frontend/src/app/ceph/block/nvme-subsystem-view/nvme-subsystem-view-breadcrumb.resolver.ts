import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class NvmeSubsystemViewBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const subsystemNQN = route.parent?.params?.subsystem_nqn || route.params?.subsystem_nqn;
    return [{ text: decodeURIComponent(subsystemNQN || ''), path: null }];
  }
}
