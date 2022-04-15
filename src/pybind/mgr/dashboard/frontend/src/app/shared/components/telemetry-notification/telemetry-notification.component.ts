import { Component, OnDestroy, OnInit } from '@angular/core';

import _ from 'lodash';
import { CookieService } from 'ngx-cookie-service';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TelemetryNotificationService } from '~/app/shared/services/telemetry-notification.service';

@Component({
  selector: 'cd-telemetry-notification',
  templateUrl: './telemetry-notification.component.html',
  styleUrls: ['./telemetry-notification.component.scss']
})
export class TelemetryNotificationComponent implements OnInit, OnDestroy {
  displayNotification = false;
  notificationSeverity = 'warning';

  constructor(
    private mgrModuleService: MgrModuleService,
    private authStorageService: AuthStorageService,
    private notificationService: NotificationService,
    private telemetryNotificationService: TelemetryNotificationService,
    private cookieService: CookieService
  ) {}

  ngOnInit() {
    this.telemetryNotificationService.update.subscribe((visible: boolean) => {
      this.displayNotification = visible;
    });

    if (!this.isNotificationHidden()) {
      const configOptPermissions = this.authStorageService.getPermissions().configOpt;
      if (_.every(Object.values(configOptPermissions))) {
        this.mgrModuleService.getConfig('telemetry').subscribe((options) => {
          if (!options['enabled']) {
            this.telemetryNotificationService.setVisibility(true);
          }
        });
      }
    }
  }

  ngOnDestroy() {
    this.telemetryNotificationService.setVisibility(false);
  }

  isNotificationHidden(): boolean {
    const myDate: Date = new Date();
    myDate.setHours( myDate.getHours() + 1 );
    return this.cookieService.get('telemetry_notification_hidden') === 'true';
  }

  onDismissed(): void {
    this.telemetryNotificationService.setVisibility(false);
    this.cookieService.set('telemetry_notification_hidden', 'true', 365);
    this.notificationService.show(
      NotificationType.success,
      $localize`Telemetry activation reminder muted`,
      $localize`You can activate the module on the Telemetry configuration \
page (<b>Dashboard Settings</b> -> <b>Telemetry configuration</b>) at any time.`
    );
  }
}
