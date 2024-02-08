import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/internal/Observable';
import { catchError, map } from 'rxjs/operators';
import { intersection, isEqual, uniqWith } from 'lodash';
import { SnapshotSchedule } from '../models/snapshot-schedule';
import { of } from 'rxjs';
import {
  RepeaFrequencyPlural,
  RepeaFrequencySingular,
  RepeatFrequency
} from '../enum/repeat-frequency.enum';
import { RetentionFrequencyCopy } from '../enum/retention-frequency.enum';

@Injectable({
  providedIn: 'root'
})
export class CephfsSnapshotScheduleService {
  baseURL = 'api/cephfs';

  constructor(private http: HttpClient) {}

  create(data: Record<string, any>): Observable<any> {
    return this.http.post(`${this.baseURL}/snapshot/schedule`, data, { observe: 'response' });
  }

  update({ fs, path, ...rest }: Record<string, any>): Observable<any> {
    return this.http.put(
      `${this.baseURL}/snapshot/schedule/${fs}/${encodeURIComponent(path)}`,
      rest,
      { observe: 'response' }
    );
  }

  activate({ fs, path, ...rest }: Record<string, any>): Observable<any> {
    return this.http.post(
      `${this.baseURL}/snapshot/schedule/${fs}/${encodeURIComponent(path)}/activate`,
      rest,
      { observe: 'response' }
    );
  }

  deactivate({ fs, path, ...rest }: Record<string, any>): Observable<any> {
    return this.http.post(
      `${this.baseURL}/snapshot/schedule/${fs}/${encodeURIComponent(path)}/deactivate`,
      rest,
      { observe: 'response' }
    );
  }

  delete({ fs, path, schedule, start, subvol, group }: Record<string, any>): Observable<any> {
    let deleteUrl = `${this.baseURL}/snapshot/schedule/${fs}/${encodeURIComponent(
      path
    )}/delete_snapshot?schedule=${schedule}&start=${encodeURIComponent(start)}`;
    if (subvol && group) {
      deleteUrl += `&subvol=${encodeURIComponent(subvol)}&group=${encodeURIComponent(group)}`;
    }
    return this.http.delete(deleteUrl);
  }

  checkScheduleExists(
    path: string,
    fs: string,
    interval: number,
    frequency: RepeatFrequency
  ): Observable<boolean> {
    return this.getSnapshotScheduleList(path, fs, false).pipe(
      map((response) => {
        const index = response.findIndex(
          (x) => x.path === path && x.schedule === `${interval}${frequency}`
        );
        return index > -1;
      }),
      catchError(() => {
        return of(false);
      })
    );
  }

  checkRetentionPolicyExists(
    path: string,
    fs: string,
    retentionFrequencies: string[],
    retentionFrequenciesRemoved: string[] = []
  ): Observable<{ exists: boolean; errorIndex: number }> {
    return this.getSnapshotSchedule(path, fs, false).pipe(
      map((response) => {
        let errorIndex = -1;
        let exists = false;
        const index = response.findIndex((x) => x.path === path);
        const result = retentionFrequencies?.length
          ? intersection(
              Object.keys(response?.[index]?.retention).filter(
                (v) => !retentionFrequenciesRemoved.includes(v)
              ),
              retentionFrequencies
            )
          : [];
        exists = !!result?.length;
        result?.forEach((r) => (errorIndex = retentionFrequencies.indexOf(r)));

        return { exists, errorIndex };
      }),
      catchError(() => {
        return of({ exists: false, errorIndex: -1 });
      })
    );
  }

  getSnapshotSchedule(path: string, fs: string, recursive = true): Observable<SnapshotSchedule[]> {
    return this.http
      .get<SnapshotSchedule[]>(
        `${this.baseURL}/snapshot/schedule/${fs}?path=${path}&recursive=${recursive}`
      )
      .pipe(
        catchError(() => {
          return of([]);
        })
      );
  }

  getSnapshotScheduleList(
    path: string,
    fs: string,
    recursive = true
  ): Observable<SnapshotSchedule[]> {
    return this.getSnapshotSchedule(path, fs, recursive).pipe(
      map((snapList: SnapshotSchedule[]) =>
        uniqWith(
          snapList.map((snapItem: SnapshotSchedule) => ({
            ...snapItem,
            scheduleCopy: this.parseScheduleCopy(snapItem.schedule),
            status: snapItem.active ? 'Active' : 'Inactive',
            subvol: snapItem?.subvol,
            retentionCopy: this.parseRetentionCopy(snapItem?.retention),
            retention: Object.values(snapItem?.retention || [])?.length
              ? Object.entries(snapItem.retention)
                  ?.map?.(([frequency, interval]) => `${interval}${frequency.toLocaleUpperCase()}`)
                  .join(' ')
              : '-'
          })),
          isEqual
        )
      )
    );
  }

  parseScheduleCopy(schedule: string): string {
    const scheduleArr = schedule.split('');
    const interval = Number(scheduleArr.filter((x) => !isNaN(Number(x))).join(''));
    const frequencyUnit = scheduleArr[scheduleArr.length - 1];
    const frequency =
      interval > 1 ? RepeaFrequencyPlural[frequencyUnit] : RepeaFrequencySingular[frequencyUnit];
    return $localize`Every ${interval > 1 ? interval + ' ' : ''}${frequency}`;
  }

  parseRetentionCopy(retention: string | Record<string, number>): string[] {
    if (!retention) return ['-'];
    return Object.entries(retention).map(([frequency, interval]) =>
      $localize`${interval} ${RetentionFrequencyCopy[frequency]}`.toLocaleLowerCase()
    );
  }
}
