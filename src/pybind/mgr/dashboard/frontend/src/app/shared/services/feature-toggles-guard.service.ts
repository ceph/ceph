import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivate, CanActivateChild, Router } from '@angular/router';

import { map } from 'rxjs/operators';

import { FeatureTogglesMap, FeatureTogglesService } from './feature-toggles.service';
import { ServicesModule } from './services.module';

@Injectable({
  providedIn: ServicesModule
})
export class FeatureTogglesGuardService implements CanActivate, CanActivateChild {
  constructor(private router: Router, private featureToggles: FeatureTogglesService) {}

  canActivate(route: ActivatedRouteSnapshot) {
    return this.featureToggles.get().pipe(
      map((enabledFeatures: FeatureTogglesMap) => {
        if (enabledFeatures[route.routeConfig.path] === false) {
          this.router.navigate(['404']);
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
