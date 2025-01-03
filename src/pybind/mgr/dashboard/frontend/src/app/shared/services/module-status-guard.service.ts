import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivate, CanActivateChild, Router } from '@angular/router';

import { of as observableOf } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { Icons } from '~/app/shared/enum/icons.enum';

/**
 * This service checks if a route can be activated by executing a
 * REST API call to '/ui-api/<uiApiPath>/status'. If the returned response
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
 *       uiApiPath: 'rgw',
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
  // TODO: Hotfix - remove ALLOWLIST'ing when a generic ErrorComponent is implemented
  static readonly ALLOWLIST: string[] = ['501'];

  constructor(
    private http: HttpClient,
    private router: Router,
    private mgrModuleService: MgrModuleService
  ) {}

  canActivate(route: ActivatedRouteSnapshot) {
    return this.doCheck(route);
  }

  canActivateChild(childRoute: ActivatedRouteSnapshot) {
    return this.doCheck(childRoute);
  }

  private doCheck(route: ActivatedRouteSnapshot) {
    if (route.url.length > 0 && ModuleStatusGuardService.ALLOWLIST.includes(route.url[0].path)) {
      return observableOf(true);
    }
    const config = route.data['moduleStatusGuardConfig'];
    let backendCheck = false;
    if (config.backend) {
      this.mgrModuleService.getConfig('orchestrator').subscribe(
        (resp) => {
          backendCheck = config.backend === resp['orchestrator'];
        },
        () => {
          this.router.navigate([config.redirectTo]);
          return observableOf(false);
        }
      );
    }
    return this.http.get(`ui-api/${config.uiApiPath}/status`).pipe(
      map((resp: any) => {
        if (!resp.available && !backendCheck) {
          this.router.navigate([config.redirectTo || ''], {
            state: {
              header: config.header,
              message: resp.message,
              section: config.section,
              section_info: config.section_info,
              button_name: config.button_name,
              button_route: config.button_route,
              button_title: config.button_title,
              secondary_button_name: config.secondary_button_name,
              secondary_button_route: config.secondary_button_route,
              secondary_button_title: config.secondary_button_title,
              uiConfig: config.uiConfig,
              uiApiPath: config.uiApiPath,
              icon: Icons.wrench,
              component: config.component
            }
          });
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
