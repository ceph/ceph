import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { CdPwdExpirationSettings } from '../models/cd-pwd-expiration-settings';
import { ApiModule } from './api.module';

class SettingResponse {
  name: string;
  default: any;
  type: string;
  value: any;
}

@Injectable({
  providedIn: ApiModule
})
export class SettingsService {
  constructor(private http: HttpClient) {}

  private settings: { [url: string]: string } = {};

  getValues(names: string | string[]): Observable<{ [key: string]: any }> {
    if (_.isArray(names)) {
      names = names.join(',');
    }
    return this.http.get(`api/settings?names=${names}`).pipe(
      map((resp: SettingResponse[]) => {
        const result = {};
        _.forEach(resp, (option: SettingResponse) => {
          _.set(result, option.name, option.value);
        });
        return result;
      })
    );
  }

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
  disableSetting(url: string) {
    this.settings[url] = '';
  }

  private getSettingsValue(data: any): string {
    return data.value || data.instance || '';
  }

  validateGrafanaDashboardUrl(uid: string) {
    return this.http.get(`api/grafana/validation/${uid}`);
  }

  pwdExpirationSettings(): Observable<CdPwdExpirationSettings> {
    return this.getValues([
      'USER_PWD_EXPIRATION_SPAN',
      'USER_PWD_EXPIRATION_WARNING_1',
      'USER_PWD_EXPIRATION_WARNING_2'
    ]).pipe(
      map((resp: { [key: string]: any }) => {
        const result = new CdPwdExpirationSettings(
          resp['USER_PWD_EXPIRATION_SPAN'],
          resp['USER_PWD_EXPIRATION_WARNING_1'],
          resp['USER_PWD_EXPIRATION_WARNING_2']
        );
        return result;
      })
    );
  }
}
