import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BlockUIService } from 'ng-block-ui';

import { Observable, timer as observableTimer } from 'rxjs';
import { NotificationService } from '../services/notification.service';
import { TableComponent } from '../datatable/table/table.component';
import { Router } from '@angular/router';
import { MgrModuleInfo } from '../models/mgr-modules.interface';
import { NotificationType } from '../enum/notification-type.enum';

@Injectable({
  providedIn: 'root'
})
export class MgrModuleService {
  private url = 'api/mgr/module';

  readonly REFRESH_INTERVAL = 2000;

  constructor(
    private blockUI: BlockUIService,
    private http: HttpClient,
    private notificationService: NotificationService,
    private router: Router
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
    navigateByUrl?: boolean
  ): void {
    let $obs;
    const fnWaitUntilReconnected = () => {
      observableTimer(this.REFRESH_INTERVAL).subscribe(() => {
        // Trigger an API request to check if the connection is
        // re-established.
        this.list().subscribe(
          () => {
            // Resume showing the notification toasties.
            this.notificationService.suspendToasties(false);
            // Unblock the whole UI.
            this.blockUI.stop('global');
            // Reload the data table content.
            if (table) {
              table.refreshBtn();
            }

            if (notificationText) {
              this.notificationService.show(
                NotificationType.success,
                $localize`${notificationText}`
              );
            }

            if (!navigateTo) return;

            const navigate = () => this.router.navigate([navigateTo]);

            if (navigateByUrl) {
              this.router.navigateByUrl('/', { skipLocationChange: true }).then(navigate);
            } else {
              navigate();
            }
          },
          () => {
            fnWaitUntilReconnected();
          }
        );
      });
    };

    // Note, the Ceph Mgr is always restarted when a module
    // is enabled/disabled.
    if (enabled) {
      $obs = this.disable(module);
    } else {
      $obs = this.enable(module);
    }
    $obs.subscribe(
      () => undefined,
      () => {
        // Suspend showing the notification toasties.
        this.notificationService.suspendToasties(true);
        // Block the whole UI to prevent user interactions until
        // the connection to the backend is reestablished
        this.blockUI.start('global', $localize`Reconnecting, please wait ...`);
        fnWaitUntilReconnected();
      }
    );
  }
}
