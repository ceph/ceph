import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { BreadcrumbsResolver, IBreadcrumb } from '~/app/shared/models/breadcrumbs';
import { ImageSpec } from '~/app/shared/models/image-spec';

@Injectable({
  providedIn: 'root'
})
export class RbdImageResourceBreadcrumbResolver extends BreadcrumbsResolver {
  resolve(route: ActivatedRouteSnapshot): IBreadcrumb[] {
    const imageName = this.getImageNameFromRoute(route.paramMap.get('image_spec') || '');

    return [{ text: imageName, path: this.getFullPath(route) }];
  }

  private getImageNameFromRoute(imageSpecRoute: string): string {
    if (!imageSpecRoute) {
      return '';
    }

    try {
      const imageSpec = ImageSpec.fromString(decodeURIComponent(imageSpecRoute));
      return imageSpec.imageName;
    } catch {
      return imageSpecRoute;
    }
  }
}
