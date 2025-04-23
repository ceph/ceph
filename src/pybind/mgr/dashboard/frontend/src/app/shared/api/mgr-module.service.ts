import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BlockUIService } from 'ng-block-ui';

import { Observable, Subject, timer } from 'rxjs';
import { NotificationService } from '../services/notification.service';
import { TableComponent } from '../datatable/table/table.component';
import { Router } from '@angular/router';
import { MgrModuleInfo } from '../models/mgr-modules.interface';
import { NotificationType } from '../enum/notification-type.enum';
import { delay, retryWhen, switchMap, tap } from 'rxjs/operators';
import { SummaryService } from '../services/summary.service';

const GLOBAL = 'global';

@Injectable({
  providedIn: 'root'
})
export class MgrModuleService {
  private url = 'api/mgr/module';
  updateCompleted$ = new Subject<void>();

  readonly REFRESH_INTERVAL = 2000;

  constructor(
    private blockUI: BlockUIService,
    private http: HttpClient,
    private notificationService: NotificationService,
    private router: Router,
    private summaryService: SummaryService
  ) {}

  /**
   * Get the list of Ceph Mgr modules and their state (enabled/disabled).
   * @return {Observable<Object[]>}
   */
  list(): Observable<MgrModuleInfo[]> {
    return this.http.get<MgrModuleInfo[]>(`${this.url}`);
  }

  /**
   * Get the Ceph Mgr module configuration.
   * @param {string} module The name of the mgr module.
   * @return {Observable<Object>}
   */
  getConfig(module: string): Observable<Object> {
    return this.http.get(`${this.url}/${module}`);
  }

  /**
   * Update the Ceph Mgr module configuration.
   * @param {string} module The name of the mgr module.
   * @param {object} config The configuration.
   * @return {Observable<Object>}
   */
  updateConfig(module: string, config: object): Observable<Object> {
    return this.http.put(`${this.url}/${module}`, { config: config });
  }

  /**
   * Enable the Ceph Mgr module.
   * @param {string} module The name of the mgr module.
   */
  enable(module: string) {
    return this.http.post(`${this.url}/${module}/enable`, null);
  }

  /**
   * Disable the Ceph Mgr module.
   * @param {string} module The name of the mgr module.
   */
  disable(module: string) {
    return this.http.post(`${this.url}/${module}/disable`, null);
  }

  /**
   * Get the Ceph Mgr module options.
   * @param {string} module The name of the mgr module.
   * @return {Observable<Object>}
   */
  getOptions(module: string): Observable<Object> {
    return this.http.get(`${this.url}/${module}/options`);
  }

  /**
   * Update the Ceph Mgr module state to enabled or disabled.
   */
  updateModuleState(
    module: string,
    enabled: boolean = false,
    table: TableComponent = null,
    navigateTo: string = '',
    notificationText?: string,
    navigateByUrl?: boolean,
    reconnectingMessage: string = $localize`Reconnecting, please wait ...`
  ): void {
    const moduleToggle$ = enabled ? this.disable(module) : this.enable(module);

    moduleToggle$.subscribe({
      next: () => {
        // Module toggle succeeded
        this.updateCompleted$.next();
      },
      error: () => {
        // Module toggle failed, trigger reconnect flow
        this.notificationService.suspendToasties(true);
        this.blockUI.start(GLOBAL, reconnectingMessage);

        timer(this.REFRESH_INTERVAL)
          .pipe(
            switchMap(() => this.list()),
            retryWhen((errors) =>
              errors.pipe(
                tap(() => {
                  // Keep retrying until list() succeeds
                }),
                delay(this.REFRESH_INTERVAL)
              )
            )
          )
          .subscribe({
            next: () => {
              // Reconnection successful
              this.notificationService.suspendToasties(false);
              this.blockUI.stop(GLOBAL);

              if (table) {
                table.refreshBtn();
              }

              if (notificationText) {
                this.notificationService.show(
                  NotificationType.success,
                  $localize`${notificationText}`
                );
              }

              if (navigateTo) {
                const navigate = () => this.router.navigate([navigateTo]);
                if (navigateByUrl) {
                  this.router.navigateByUrl('/', { skipLocationChange: true }).then(navigate);
                } else {
                  navigate();
                }
              }

              this.updateCompleted$.next();
              this.summaryService.startPolling();
            }
          });
      }
    });
  }
}
