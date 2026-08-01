import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';
import { decodeModuleName } from '~/app/shared/models/mgr-modules.interface';

@Injectable({
  providedIn: 'root'
})
export class MgrModuleResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const moduleName = decodeModuleName(route.paramMap.get('name') || '');

    return [{ text: moduleName, path: this.getFullPath(route) }];
  }
}
