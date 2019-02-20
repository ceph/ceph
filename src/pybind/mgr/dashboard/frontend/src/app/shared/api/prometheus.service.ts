import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { PrometheusAlert, PrometheusNotification } from '../models/prometheus-alerts';
import { PrometheusSilence } from '../models/prometheus-silence';
import { ApiModule } from './api.module';
import { SettingsService } from './settings.service';

@Injectable({
  providedIn: ApiModule
})
export class PrometheusService {
  private baseURL = 'api/prometheus';
  private settingsKey = {
    alertmanager: 'api/settings/alertmanager-api-host',
    prometheus: 'api/settings/prometheus-api-host'
  };

  constructor(private http: HttpClient, private settingsService: SettingsService) {}

  ifAlertmanagerConfigured(fn): void {
    this.settingsService.ifSettingConfigured(this.settingsKey.alertmanager, fn);
  }

  disableAlertmanagerConfig(): void {
    this.settingsService.disableSetting(this.settingsKey.alertmanager);
  }

  ifPrometheusConfigured(fn): void {
    this.settingsService.ifSettingConfigured(this.settingsKey.prometheus, fn);
  }

  disablePrometheusConfig(): void {
    this.settingsService.disableSetting(this.settingsKey.prometheus);
  }

  list(params = {}): Observable<PrometheusAlert[]> {
    return this.http.get<PrometheusAlert[]>(this.baseURL, { params });
  }

  listSilences(params = {}): Observable<PrometheusSilence[]> {
    return this.http.get<PrometheusSilence[]>(`${this.baseURL}/silences`, { params });
  }

  setSilence(silence: PrometheusSilence) {
    return this.http.post(`${this.baseURL}/silence`, silence, { observe: 'response' });
  }

  expireSilence(silenceId: string) {
    return this.http.delete(`${this.baseURL}/silence/${silenceId}`, { observe: 'response' });
  }

  getNotificationSince(notification): Observable<PrometheusNotification[]> {
    return this.http.post<PrometheusNotification[]>(
      `${this.baseURL}/get_notifications_since`,
      notification
    );
  }
}
