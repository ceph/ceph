import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';
import { RgwTopicKeyService } from '~/app/shared/services/rgw-topic-key.service';

@Injectable({
  providedIn: 'root'
})
export class RgwTopicResourceBreadcrumbResolver extends BreadcrumbsResolver {
  constructor(private rgwTopicKeyService: RgwTopicKeyService) {
    super();
  }

  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const topicKey = route.params?.name || '';
    const topicName = this.rgwTopicKeyService.extractTopicName(topicKey);

    return [{ text: topicName, path: this.getFullPath(route) }];
  }
}
