import { Component, OnDestroy, OnInit } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { UserFormModel } from '../../../core/auth/user-form/user-form.model';
import { MgrModuleService } from '../../api/mgr-module.service';
import { UserService } from '../../api/user.service';
import { NotificationType } from '../../enum/notification-type.enum';
import { AuthStorageService } from '../../services/auth-storage.service';
import { NotificationService } from '../../services/notification.service';
import { TelemetryNotificationService } from '../../services/telemetry-notification.service';

@Component({
  selector: 'cd-telemetry-notification',
  templateUrl: './telemetry-notification.component.html',
  styleUrls: ['./telemetry-notification.component.scss']
})
export class TelemetryNotificationComponent implements OnInit, OnDestroy {
  displayNotification = false;

  constructor(
    private mgrModuleService: MgrModuleService,
    private authStorageService: AuthStorageService,
    private userService: UserService,
    private notificationService: NotificationService,
    private telemetryNotificationService: TelemetryNotificationService,
    private i18n: I18n
  ) {}

  ngOnInit() {
    this.telemetryNotificationService.update.subscribe((visible: boolean) => {
      this.displayNotification = visible;
    });

    if (!this.isNotificationHidden()) {
      const username = this.authStorageService.getUsername();
      this.userService.get(username).subscribe((user: UserFormModel) => {
        if (user.roles.includes('administrator')) {
          this.mgrModuleService.getConfig('telemetry').subscribe((options) => {
            if (!options['enabled']) {
              this.telemetryNotificationService.setVisibility(true);
            }
          });
        }
      });
    }
  }

  ngOnDestroy() {
    this.telemetryNotificationService.setVisibility(false);
  }

  isNotificationHidden(): boolean {
    return localStorage.getItem('telemetry_notification_hidden') === 'true';
  }

  close() {
    this.telemetryNotificationService.setVisibility(false);
    localStorage.setItem('telemetry_notification_hidden', 'true');
    this.notificationService.show(
      NotificationType.success,
      this.i18n('Telemetry activation reminder muted'),
      this.i18n(
        'You can activate the module on the Telemetry configuration ' +
          'page (<b>Dashboard Settings</b> -> <b>Telemetry configuration</b>) at any time.'
      )
    );
  }
}
