import { Component, HostBinding, OnDestroy, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { Subscription } from 'rxjs';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

import { FaviconService } from '~/app/shared/services/favicon.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { TaskManagerService } from '~/app/shared/services/task-manager.service';
import { TelemetryNotificationService } from '../../../shared/services/telemetry-notification.service';
import { MotdNotificationService } from '~/app/shared/services/motd-notification.service';
import _ from 'lodash';

@Component({
  selector: 'cd-workbench-layout',
  templateUrl: './workbench-layout.component.html',
  styleUrls: ['./workbench-layout.component.scss'],
  providers: [FaviconService]
})
export class WorkbenchLayoutComponent implements OnInit, OnDestroy {
  notifications: string[] = [];
  private subs = new Subscription();
  permissions: Permissions;
  @HostBinding('class') get class(): string {
    return 'top-notification-' + this.notifications.length;
  }

  constructor(
    public router: Router,
    private summaryService: SummaryService,
    private taskManagerService: TaskManagerService,
    private multiClusterService: MultiClusterService,
    private faviconService: FaviconService,
    private authStorageService: AuthStorageService,
    private telemetryNotificationService: TelemetryNotificationService,
    private motdNotificationService: MotdNotificationService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    if (this.permissions.configOpt.read) {
      this.subs.add(this.multiClusterService.startPolling());
      this.subs.add(this.multiClusterService.startClusterTokenStatusPolling());
    }
    this.subs.add(this.summaryService.startPolling());
    this.subs.add(this.taskManagerService.init(this.summaryService));

    this.subs.add(
      this.authStorageService.isPwdDisplayed$.subscribe((isDisplayed) => {
        this.showTopNotification('isPwdDisplayed', isDisplayed);
      })
    );
    this.subs.add(
      this.telemetryNotificationService.update.subscribe((visible: boolean) => {
        this.showTopNotification('telemetryNotificationEnabled', visible);
      })
    );
    this.subs.add(
      this.motdNotificationService.motd$.subscribe((motd: any) => {
        this.showTopNotification('motdNotificationEnabled', _.isPlainObject(motd));
      })
    );
    this.faviconService.init();
  }
  showTopNotification(name: string, isDisplayed: boolean) {
    if (isDisplayed) {
      if (!this.notifications.includes(name)) {
        this.notifications.push(name);
      }
    } else {
      const index = this.notifications.indexOf(name);
      if (index >= 0) {
        this.notifications.splice(index, 1);
      }
    }
  }

  ngOnDestroy() {
    this.subs.unsubscribe();
  }
}
