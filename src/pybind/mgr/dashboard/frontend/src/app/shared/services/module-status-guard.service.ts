import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import {
  ActivatedRouteSnapshot,
  CanActivate,
  CanActivateChild,
  Router,
  RouterStateSnapshot
} from '@angular/router';

import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';

/**
 * This service checks if a route can be activated by executing a
 * REST API call to '/api/<apiPath>/status'. If the returned response
 * states that the module is not available, then the user is redirected
 * to the specified <redirectTo> URL path.
 *
 * A controller implementing this endpoint should return an object of
 * the following form:
 * {'available': true|false, 'message': null|string}.
 *
 * The configuration of this guard should look like this:
 * const routes: Routes = [
 * {
 *   path: 'rgw/bucket',
 *   component: RgwBucketListComponent,
 *   canActivate: [AuthGuardService, ModuleStatusGuardService],
 *   data: {
 *     moduleStatusGuardConfig: {
 *       apiPath: 'rgw',
 *       redirectTo: 'rgw/501'
 *     }
 *   }
 * },
 * ...
 */
@Injectable()
export class ModuleStatusGuardService implements CanActivate, CanActivateChild {

  constructor(private http: HttpClient,
              private router: Router) {}

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot) {
    return this.doCheck(route);
  }

  canActivateChild(childRoute: ActivatedRouteSnapshot, state: RouterStateSnapshot) {
    return this.doCheck(childRoute);
  }

  private doCheck(route: ActivatedRouteSnapshot) {
    const config = route.data['moduleStatusGuardConfig'];
    return this.http.get(`/api/${config.apiPath}/status`)
      .map((resp: any) => {
        if (!resp.available) {
          this.router.navigate([config.redirectTo, resp.message || '']);
        }
        return resp.available;
      })
      .catch(() => {
        this.router.navigate([config.redirectTo]);
        return Observable.of(false);
      });
  }
}
