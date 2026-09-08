import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';
import { Account } from '../models/rgw-user-accounts';

@Injectable({
  providedIn: 'root'
})
export class RgwAccountDetailsBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const account = route.data?.account as Account | null;
    const accountName = account?.name || route.paramMap.get('accountName') || '';

    return [{ text: accountName, path: this.getFullPath(route) }];
  }
}
