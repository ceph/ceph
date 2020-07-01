import { Component, OnDestroy, OnInit } from '@angular/core';

import { SettingsService } from '../../api/settings.service';
import { CdPwdExpirationSettings } from '../../models/cd-pwd-expiration-settings';
import { AuthStorageService } from '../../services/auth-storage.service';

@Component({
  selector: 'cd-pwd-expiration-notification',
  templateUrl: './pwd-expiration-notification.component.html',
  styleUrls: ['./pwd-expiration-notification.component.scss']
})
export class PwdExpirationNotificationComponent implements OnInit, OnDestroy {
  alertType: string;
  expirationDays: number;
  pwdExpirationSettings: CdPwdExpirationSettings;
  displayNotification = false;

  constructor(
    private settingsService: SettingsService,
    private authStorageService: AuthStorageService
  ) {}

  ngOnInit() {
    this.settingsService.getStandardSettings().subscribe((pwdExpirationSettings) => {
      this.pwdExpirationSettings = new CdPwdExpirationSettings(pwdExpirationSettings);
      const pwdExpirationDate = this.authStorageService.getPwdExpirationDate();
      if (pwdExpirationDate) {
        this.expirationDays = this.getExpirationDays(pwdExpirationDate);
        if (this.expirationDays <= this.pwdExpirationSettings.pwdExpirationWarning2) {
          this.alertType = 'danger';
        } else {
          this.alertType = 'warning';
        }
        this.displayNotification =
          this.expirationDays <= this.pwdExpirationSettings.pwdExpirationWarning1;
        this.authStorageService.isPwdDisplayedSource.next(this.displayNotification);
      }
    });
  }

  ngOnDestroy() {
    this.authStorageService.isPwdDisplayedSource.next(false);
  }

  private getExpirationDays(pwdExpirationDate: number): number {
    const current = new Date();
    const expiration = new Date(pwdExpirationDate * 1000);
    return Math.floor((expiration.valueOf() - current.valueOf()) / (1000 * 3600 * 24));
  }

  close() {
    this.authStorageService.isPwdDisplayedSource.next(false);
    this.displayNotification = false;
  }
}
