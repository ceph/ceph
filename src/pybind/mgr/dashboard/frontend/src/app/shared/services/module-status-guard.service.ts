import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Router, RouterStateSnapshot } from '@angular/router';

import { of as observableOf } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

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
export class ModuleStatusGuardService {
  // TODO: Hotfix - remove ALLOWLIST'ing when a generic ErrorComponent is implemented
  static readonly ALLOWLIST: string[] = ['501'];

  constructor(
    private http: HttpClient,
    private router: Router,
    private mgrModuleService: MgrModuleService,
    private authStorageService: AuthStorageService
  ) {}

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot) {
    return this.doCheck(route, state);
  }

  canActivateChild(childRoute: ActivatedRouteSnapshot, state: RouterStateSnapshot) {
    return this.doCheck(childRoute, state);
  }

  /**
   * Allow only /add-storage?welcome=true through without an orch status check.
   * Other modules must not bypass the guard via ?welcome=true.
   */
  private isAddStorageWelcome(route: ActivatedRouteSnapshot, state?: RouterStateSnapshot): boolean {
    const isAddStorage =
      route.routeConfig?.path === 'add-storage' ||
      route.url?.[0]?.path === 'add-storage' ||
      !!state?.url?.match(/^\/add-storage(?:\?|$)/);
    if (!isAddStorage) {
      return false;
    }
    const welcome = route.queryParams?.['welcome'];
    if (welcome === true || welcome === 'true') {
      return true;
    }
    return !!state?.url && /(?:\?|&)welcome=true(?:&|$)/.test(state.url);
  }

  private doCheck(route: ActivatedRouteSnapshot, state?: RouterStateSnapshot) {
    if (route.url.length > 0 && ModuleStatusGuardService.ALLOWLIST.includes(route.url[0].path)) {
      return observableOf(true);
    }
    // Login → /add-storage?welcome=true must not be bounced to overview when orch
    // is not ready yet. Scoped to add-storage so other modules stay guarded.
    if (this.isAddStorageWelcome(route, state)) {
      return observableOf(true);
    }
    const config = route.data['moduleStatusGuardConfig'];
    let backendCheck = false;
    if (config.backend && this.authStorageService.getPermissions().configOpt?.read) {
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
              module_name: config.module_name,
              navigate_to: config.navigate_to,
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
