import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

class SettingResponse {
  name: string;
  default: any;
  type: string;
  value: any;
}

@Injectable({
  providedIn: 'root'
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

  ifSettingConfigured(
    url: string,
    fn: (value?: string) => void,
    elseFn?: () => void,
    forceRefresh = false
  ): void {
    const setting = this.settings[url];
    if (forceRefresh || setting === undefined) {
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

  getStandardSettings(): Observable<{ [key: string]: any }> {
    return this.http.get('ui-api/standard_settings');
  }
}
