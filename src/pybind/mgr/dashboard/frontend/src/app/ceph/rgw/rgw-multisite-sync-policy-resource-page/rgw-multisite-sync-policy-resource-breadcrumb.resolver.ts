import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';

@Injectable({
  providedIn: 'root'
})
export class RgwMultisiteSyncPolicyResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const groupName = route.paramMap.get('groupName') || '';
    const bucketName = route.queryParamMap.get('bucketName') || '';
    const encodedGroupName = encodeURIComponent(groupName);
    const encodedBucketName = encodeURIComponent(bucketName);
    const resourcePath = bucketName
      ? `/rgw/multisite/sync-policy/${encodedGroupName}?bucketName=${encodedBucketName}`
      : `/rgw/multisite/sync-policy/${encodedGroupName}`;

    return [
      { text: 'Sync-policy', path: '/rgw/multisite/sync-policy' },
      { text: groupName, path: resourcePath }
    ];
  }
}
