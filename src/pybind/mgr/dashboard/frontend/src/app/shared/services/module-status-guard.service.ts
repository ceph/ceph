import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivate, CanActivateChild, Router } from '@angular/router';

import { of as observableOf } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

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
@Injectable({
  providedIn: 'root'
})
export class ModuleStatusGuardService implements CanActivate, CanActivateChild {
  // TODO: Hotfix - remove WHITELIST'ing when a generic ErrorComponent is implemented
  static readonly WHITELIST: string[] = ['501'];

  constructor(private http: HttpClient, private router: Router) {}

  canActivate(route: ActivatedRouteSnapshot) {
    return this.doCheck(route);
  }

  canActivateChild(childRoute: ActivatedRouteSnapshot) {
    return this.doCheck(childRoute);
  }

  private doCheck(route: ActivatedRouteSnapshot) {
    if (route.url.length > 0 && ModuleStatusGuardService.WHITELIST.includes(route.url[0].path)) {
      return observableOf(true);
    }
    const config = route.data['moduleStatusGuardConfig'];
    return this.http.get(`api/${config.apiPath}/status`).pipe(
      map((resp: any) => {
        if (!resp.available) {
          this.router.navigate([config.redirectTo, resp.message || '']);
        }
        return resp.available;
      }),
      catchError(() => {
        this.router.navigate([config.redirectTo]);
        return observableOf(false);
      })
    );
  }
}
