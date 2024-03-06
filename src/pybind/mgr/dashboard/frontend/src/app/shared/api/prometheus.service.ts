import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable, Subscription, forkJoin, timer } from 'rxjs';
import { map } from 'rxjs/operators';

import { AlertmanagerSilence } from '../models/alertmanager-silence';
import {
  AlertmanagerAlert,
  AlertmanagerNotification,
  PrometheusRuleGroup
} from '../models/prometheus-alerts';
import moment from 'moment';

@Injectable({
  providedIn: 'root'
})
export class PrometheusService {
  timerGetPrometheusDataSub: Subscription;
  timerTime = 30000;
  readonly lastHourDateObject = {
    start: moment().unix() - 3600,
    end: moment().unix(),
    step: 14
  };
  private baseURL = 'api/prometheus';
  private settingsKey = {
    alertmanager: 'ui-api/prometheus/alertmanager-api-host',
    prometheus: 'ui-api/prometheus/prometheus-api-host'
  };
  private settings: { [url: string]: string } = {};

  constructor(private http: HttpClient) {}

  unsubscribe() {
    if (this.timerGetPrometheusDataSub) {
      this.timerGetPrometheusDataSub.unsubscribe();
    }
  }

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

  getPrometheusQueriesData(
    selectedTime: any,
    queries: any,
    queriesResults: any,
    checkNan?: boolean
  ) {
    this.ifPrometheusConfigured(() => {
      if (this.timerGetPrometheusDataSub) {
        this.timerGetPrometheusDataSub.unsubscribe();
      }
      this.timerGetPrometheusDataSub = timer(0, this.timerTime).subscribe(() => {
        selectedTime = this.updateTimeStamp(selectedTime);

        for (const queryName in queries) {
          if (queries.hasOwnProperty(queryName)) {
            const query = queries[queryName];
            this.getPrometheusData({
              params: encodeURIComponent(query),
              start: selectedTime['start'],
              end: selectedTime['end'],
              step: selectedTime['step']
            }).subscribe((data: any) => {
              if (data.result.length) {
                queriesResults[queryName] = data.result[0].values;
              } else {
                queriesResults[queryName] = [];
              }
              if (
                queriesResults[queryName] !== undefined &&
                queriesResults[queryName] !== '' &&
                checkNan
              ) {
                queriesResults[queryName].forEach((valueArray: string[]) => {
                  if (valueArray.includes('NaN')) {
                    const index = valueArray.indexOf('NaN');
                    if (index !== -1) {
                      valueArray[index] = '0';
                    }
                  }
                });
              }
            });
          }
        }
      });
    });
    return queriesResults;
  }

  private updateTimeStamp(selectedTime: any): any {
    let formattedDate = {};
    let secondsAgo = selectedTime['end'] - selectedTime['start'];
    const date: number = moment().unix() - secondsAgo;
    const dateNow: number = moment().unix();
    formattedDate = {
      start: date,
      end: dateNow,
      step: selectedTime['step']
    };
    return formattedDate;
  }

  getMultiClusterData(params: any): any {
    return this.http.get<any>(`${this.baseURL}/prometheus_query_data`, { params });
  }

  getMultiClusterQueryRangeData(params: any): any {
    return this.http.get<any>(`${this.baseURL}/data`, { params });
  }

  getMultiClusterQueriesData(selectedTime: any, queries: any, queriesResults: any) {
    return new Observable((observer) => {
      this.ifPrometheusConfigured(() => {
        if (this.timerGetPrometheusDataSub) {
          this.timerGetPrometheusDataSub.unsubscribe();
        }

        this.timerGetPrometheusDataSub = timer(0, this.timerTime).subscribe(() => {
          selectedTime = this.updateTimeStamp(selectedTime);

          const requests = [];
          for (const queryName in queries) {
            if (queries.hasOwnProperty(queryName)) {
              const validRangeQueries1 = [
                'CLUSTER_CAPACITY_UTILIZATION',
                'CLUSTER_IOPS_UTILIZATION',
                'CLUSTER_THROUGHPUT_UTILIZATION',
                'POOL_CAPACITY_UTILIZATION',
                'POOL_IOPS_UTILIZATION',
                'POOL_THROUGHPUT_UTILIZATION'
              ];
              if (validRangeQueries1.includes(queryName)) {
                const query = queries[queryName];
                const request = this.getMultiClusterQueryRangeData({
                  params: encodeURIComponent(query),
                  start: selectedTime['start'],
                  end: selectedTime['end'],
                  step: selectedTime['step']
                });
                requests.push(request);
              } else {
                const query = queries[queryName];
                const request = this.getMultiClusterData({
                  params: encodeURIComponent(query),
                  start: selectedTime['start'],
                  end: selectedTime['end'],
                  step: selectedTime['step']
                });
                requests.push(request);
              }
            }
          }

          forkJoin(requests).subscribe(
            (responses: any[]) => {
              for (let i = 0; i < responses.length; i++) {
                const data = responses[i];
                const queryName = Object.keys(queries)[i];
                const validQueries = [
                  'ALERTS',
                  'MGR_METADATA',
                  'HEALTH_STATUS',
                  'TOTAL_CAPACITY',
                  'USED_CAPACITY',
                  'POOLS',
                  'OSDS',
                  'CLUSTER_CAPACITY_UTILIZATION',
                  'CLUSTER_IOPS_UTILIZATION',
                  'CLUSTER_THROUGHPUT_UTILIZATION',
                  'POOL_CAPACITY_UTILIZATION',
                  'POOL_IOPS_UTILIZATION',
                  'POOL_THROUGHPUT_UTILIZATION',
                  'HOSTS',
                  'CLUSTER_ALERTS'
                ];
                if (data.result.length) {
                  if (validQueries.includes(queryName)) {
                    queriesResults[queryName] = data.result;
                  } else {
                    queriesResults[queryName] = data.result.map(
                      (result: { value: any }) => result.value
                    );
                  }
                }
              }
              observer.next(queriesResults);
              observer.complete();
            },
            (error: Error) => {
              observer.error(error);
            }
          );
        });
      });
    });
  }
}
