import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class SettingsService {
  constructor(private http: HttpClient) {}

  private settings: { [url: string]: string } = {};

  ifSettingConfigured(url: string, fn: (value?: string) => void): void {
    const setting = this.settings[url];
    if (setting === undefined) {
      this.http.get(url).subscribe((data: any) => {
        this.settings[url] = this.getSettingsValue(data);
        this.ifSettingConfigured(url, fn);
      });
    } else if (setting !== '') {
      fn(setting);
    }
  }

  private getSettingsValue(data: any): string {
    return data.value || data.instance || '';
  }

  validateGrafanaDashboardUrl(uid) {
    return this.http.get(`api/grafana/validation/${uid}`);
  }
}
