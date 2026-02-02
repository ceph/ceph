import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class NvmeSubsystemViewBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
<<<<<<< HEAD
    const subsystemNQN = route.parent?.params?.subsystem_nqn || route.params?.subsystem_nqn || '';
    let decodedNQN = subsystemNQN;
    try {
      decodedNQN = decodeURIComponent(subsystemNQN);
    } catch (e) {
      // Fallback to raw value if decoding fails
    }
    return [{ text: decodedNQN, path: null }];
=======
    const subsystemNQN = route.parent?.params?.subsystem_nqn || route.params?.subsystem_nqn;
    return [{ text: decodeURIComponent(subsystemNQN || ''), path: null }];
>>>>>>> 50177830211 (mgr/dashboard: NVMe – Fix host,listeners namespace list display on Subsystem resource page)
  }
}
