import { Component, Inject, OnInit, Optional } from '@angular/core';
import { Router } from '@angular/router';
import { BaseModal } from 'carbon-components-angular';
import { of, forkJoin, timer } from 'rxjs';
import { catchError, switchMap } from 'rxjs/operators';
import { DaemonService } from '~/app/shared/api/daemon.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';

export interface ServiceRestartItem {
  name: string;
  status: 'pending' | 'restarting' | 'restarted' | 'failed';
  tagType:
    'gray' | 'cool-gray' | 'warm-gray' | 'magenta' | 'blue' | 'cyan' | 'teal' | 'green' | 'red';
  statusText: string;
}

@Component({
  selector: 'cd-config-option-restart-modal',
  templateUrl: './config-option-restart-modal.component.html',
  styleUrls: ['./config-option-restart-modal.component.scss'],
  standalone: false
})
export class ConfigOptionRestartModalComponent extends BaseModal implements OnInit {
  serviceItems: ServiceRestartItem[] = [];
  restartingAll = false;

  constructor(
    @Optional() @Inject('configName') public configName = '',
    @Optional() @Inject('services') public services: string[] = [],
    private daemonService: DaemonService,
    private notificationService: NotificationService,
    private router: Router
  ) {
    super();
  }

  ngOnInit() {
    const rawServices =
      this.services && this.services.length > 0 ? this.services : ['affected service'];
    this.serviceItems = rawServices.map((s) => ({
      name: s,
      status: 'pending',
      tagType: 'blue',
      statusText: 'Pending'
    }));
  }

  get allRestarted(): boolean {
    return this.serviceItems.every((item) => item.status === 'restarted');
  }

  get hasMgrService(): boolean {
    return this.serviceItems.some((item) => item.name.toLowerCase().includes('mgr'));
  }

  restartSingleService(item: ServiceRestartItem) {
    if (item.status === 'restarting' || item.status === 'restarted') {
      return;
    }
    item.status = 'restarting';
    item.tagType = 'magenta';
    item.statusText = 'Restarting...';

    const serviceName = item.name;
    this.daemonService
      .list([serviceName])
      .pipe(
        switchMap((daemons) => {
          if (daemons && daemons.length > 0) {
            const restartObservables = daemons
              .map((d) => d.daemon_name || (d as any).name)
              .filter(Boolean)
              .map((daemonName) =>
                this.daemonService
                  .action(daemonName, 'restart', true)
                  .pipe(catchError(() => of(null)))
              );

            if (restartObservables.length > 0) {
              return forkJoin(restartObservables);
            }
          }
          return of(null);
        })
      )
      .subscribe({
        next: () => {
          item.status = 'restarted';
          item.tagType = 'green';
          item.statusText = 'Restarted';
          this.notificationService.show(
            NotificationType.success,
            $localize`Initiated restart for ${serviceName} daemon(s).`
          );
        },
        error: () => {
          item.status = 'failed';
          item.tagType = 'red';
          item.statusText = 'Failed';
        }
      });
  }

  restartAllServices() {
    this.restartingAll = true;
    const pendingItems = this.serviceItems.filter((i) => i.status !== 'restarted');
    pendingItems.forEach((i) => {
      i.status = 'restarting';
      i.tagType = 'magenta';
      i.statusText = 'Restarting...';
    });

    const pendingNames = pendingItems.map((i) => i.name);
    const includesMgr = pendingNames.some((name) => name.toLowerCase().includes('mgr'));

    this.daemonService
      .list(pendingNames)
      .pipe(
        switchMap((daemons) => {
          if (daemons && daemons.length > 0) {
            const restartObservables = daemons
              .map((d) => d.daemon_name || (d as any).name)
              .filter(Boolean)
              .map((daemonName) =>
                this.daemonService
                  .action(daemonName, 'restart', true)
                  .pipe(catchError(() => of(null)))
              );

            if (restartObservables.length > 0) {
              return forkJoin(restartObservables);
            }
          }
          return of(null);
        })
      )
      .subscribe({
        next: () => {
          this.restartingAll = false;
          pendingItems.forEach((i) => {
            i.status = 'restarted';
            i.tagType = 'green';
            i.statusText = 'Restarted';
          });
          const serviceText = pendingNames.join(', ');
          this.notificationService.show(
            NotificationType.success,
            $localize`Updated config option ${this.configName} and initiated restart for ${serviceText} service daemon(s).`
          );
          this.closeModal();
          if (includesMgr) {
            timer(10000).subscribe(() => {
              this.router.navigate(['/configuration']);
            });
          } else {
            this.router.navigate(['/configuration']);
          }
        },
        error: () => {
          this.restartingAll = false;
          pendingItems.forEach((i) => {
            i.status = 'failed';
            i.tagType = 'red';
            i.statusText = 'Failed';
          });
        }
      });
  }

  skipRestart() {
    this.closeModal();
    if (!this.allRestarted) {
      const serviceText =
        this.services.length > 0 ? this.services.join(', ') : $localize`affected service(s)`;
      this.notificationService.show(
        NotificationType.success,
        $localize`Updated config option ${this.configName}. Please remember to restart ${serviceText} service(s) later.`
      );
    }
    this.router.navigate(['/configuration']);
  }
}
