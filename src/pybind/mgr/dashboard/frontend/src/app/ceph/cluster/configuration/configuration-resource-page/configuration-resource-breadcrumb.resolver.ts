import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class ConfigurationResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const configOption = this.getConfigOptionName(route.paramMap.get('name') || '');

    return [
      { text: $localize`Administration/Configuration`, path: '/configuration' },
      { text: configOption, path: this.getFullPath(route), disableSplit: true }
    ];
  }

  private getConfigOptionName(configOptionRoute: string): string {
    if (!configOptionRoute) {
      return '';
    }

    try {
      return decodeURIComponent(configOptionRoute);
    } catch {
      return configOptionRoute;
    }
  }
}
