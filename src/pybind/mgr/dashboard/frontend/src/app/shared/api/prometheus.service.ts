import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { AlertmanagerSilence } from '../models/alertmanager-silence';
import {
  AlertmanagerAlert,
  AlertmanagerNotification,
  PrometheusRuleGroup
} from '../models/prometheus-alerts';

@Injectable({
  providedIn: 'root'
})
export class PrometheusService {
  private baseURL = 'api/prometheus';
  private settingsKey = {
    alertmanager: 'ui-api/prometheus/alertmanager-api-host',
    prometheus: 'ui-api/prometheus/prometheus-api-host'
  };
  private settings: { [url: string]: string } = {};

  constructor(private http: HttpClient) {}

  getPrometheusData(params: any): any {
    return this.http.get<any>(`${this.baseURL}/data`, { params });
  }

  ifAlertmanagerConfigured(fn: (value?: string) => void, elseFn?: () => void): void {
    this.ifSettingConfigured(this.settingsKey.alertmanager, fn, elseFn);
  }

  disableAlertmanagerConfig(): void {
    this.disableSetting(this.settingsKey.alertmanager);
  }

  ifPrometheusConfigured(fn: (value?: string) => void, elseFn?: () => void): void {
    this.ifSettingConfigured(this.settingsKey.prometheus, fn, elseFn);
  }

  disablePrometheusConfig(): void {
    this.disableSetting(this.settingsKey.prometheus);
  }

  getAlerts(params = {}): Observable<AlertmanagerAlert[]> {
    return this.http.get<AlertmanagerAlert[]>(this.baseURL, { params });
  }

  getSilences(params = {}): Observable<AlertmanagerSilence[]> {
    return this.http.get<AlertmanagerSilence[]>(`${this.baseURL}/silences`, { params });
  }

  getRules(
    type: 'all' | 'alerting' | 'rewrites' = 'all'
  ): Observable<{ groups: PrometheusRuleGroup[] }> {
    return this.http.get<{ groups: PrometheusRuleGroup[] }>(`${this.baseURL}/rules`).pipe(
      map((rules) => {
        if (['alerting', 'rewrites'].includes(type)) {
          rules.groups.map((group) => {
            group.rules = group.rules.filter((rule) => rule.type === type);
          });
        }
        return rules;
      })
    );
  }

  setSilence(silence: AlertmanagerSilence) {
    return this.http.post<object>(`${this.baseURL}/silence`, silence, { observe: 'response' });
  }

  expireSilence(silenceId: string) {
    return this.http.delete(`${this.baseURL}/silence/${silenceId}`, { observe: 'response' });
  }

  getNotifications(
    notification?: AlertmanagerNotification
  ): Observable<AlertmanagerNotification[]> {
    const url = `${this.baseURL}/notifications?from=${
      notification && notification.id ? notification.id : 'last'
    }`;
    return this.http.get<AlertmanagerNotification[]>(url);
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
}
