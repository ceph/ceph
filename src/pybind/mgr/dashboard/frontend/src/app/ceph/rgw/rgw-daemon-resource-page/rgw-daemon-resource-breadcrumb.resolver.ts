import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class RgwDaemonResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const daemon = route.data?.daemon as RgwDaemon | null;
    const daemonTitle =
      daemon?.server_hostname || daemon?.id || route.paramMap.get('daemonId') || '';
    const fullPath = this.getFullPath(route);
    const gatewaysPath = fullPath.split('/').slice(0, 3).join('/');

    return [
      { text: $localize`Gateways`, path: gatewaysPath },
      { text: daemonTitle, path: fullPath }
    ];
  }
}
