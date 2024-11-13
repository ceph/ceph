import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot } from '@angular/router';

import { map } from 'rxjs/operators';

import { DashboardNotFoundError } from '~/app/core/error/error';
import { FeatureTogglesMap, FeatureTogglesService } from './feature-toggles.service';

@Injectable({
  providedIn: 'root'
})
export class FeatureTogglesGuardService {
  constructor(private featureToggles: FeatureTogglesService) {}

  canActivate(route: ActivatedRouteSnapshot) {
    return this.featureToggles.get().pipe(
      map((enabledFeatures: FeatureTogglesMap) => {
        if (enabledFeatures[route.routeConfig.path] === false) {
          throw new DashboardNotFoundError();
          return false;
        }
        return true;
      })
    );
  }

  canActivateChild(route: ActivatedRouteSnapshot) {
    return this.canActivate(route.parent);
  }
}
