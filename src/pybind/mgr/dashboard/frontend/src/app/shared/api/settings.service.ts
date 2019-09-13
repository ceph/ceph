import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { CdPwdExpirationSettings } from '../models/cd-pwd-expiration-settings';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class SettingsService {
  constructor(private http: HttpClient) {}

  private settings: { [url: string]: string } = {};

  ifSettingConfigured(url: string, fn: (value?: string) => void, elseFn?: () => void): void {
    const setting = this.settings[url];
    if (setting === undefined) {
      this.http.get(url).subscribe(
        (data: any) => {
          this.settings[url] = this.getSettingsValue(data);
          this.ifSettingConfigured(url, fn, elseFn);
        },
        (resp) => {
          if (resp.status !== 401) {
            this.settings[url] = '';
          }
        }
      );
    } else if (setting !== '') {
      fn(setting);
    } else {
      if (elseFn) {
        elseFn();
      }
    }
  }

  // Easiest way to stop reloading external content that can't be reached
  disableSetting(url) {
    this.settings[url] = '';
  }

  private getSettingsValue(data: any): string {
    return data.value || data.instance || '';
  }

  validateGrafanaDashboardUrl(uid) {
    return this.http.get(`api/grafana/validation/${uid}`);
  }

  pwdExpirationSettings(): Observable<CdPwdExpirationSettings> {
    return this.http.get<CdPwdExpirationSettings>('ui-api/standard_settings');
  }
}
