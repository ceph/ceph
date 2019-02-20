import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivate, CanActivateChild, Router } from '@angular/router';

import { of as observableOf } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { ServicesModule } from './services.module';

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
  providedIn: ServicesModule
})
export class ModuleStatusGuardService implements CanActivate, CanActivateChild {
  constructor(private http: HttpClient, private router: Router) {}

  canActivate(route: ActivatedRouteSnapshot) {
    return this.doCheck(route);
  }

  canActivateChild(childRoute: ActivatedRouteSnapshot) {
    return this.doCheck(childRoute);
  }

  private doCheck(route: ActivatedRouteSnapshot) {
    const config = route.data['moduleStatusGuardConfig'];
    return this.http.get(`/api/${config.apiPath}/status`).pipe(
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
