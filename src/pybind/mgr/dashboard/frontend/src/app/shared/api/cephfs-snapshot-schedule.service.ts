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

  delete({
    fs,
    path,
    schedule,
    start,
    retentionPolicy,
    subvol,
    group
  }: Record<string, any>): Observable<any> {
    let deleteUrl = `${this.baseURL}/snapshot/schedule/${fs}/${encodeURIComponent(
      path
    )}/delete_snapshot?schedule=${schedule}&start=${encodeURIComponent(start)}`;
    if (retentionPolicy) {
      deleteUrl += `&retention_policy=${retentionPolicy}`;
    }
    if (subvol && group) {
      deleteUrl += `&subvol=${encodeURIComponent(subvol)}&group=${encodeURIComponent(group)}`;
    }
    return this.http.delete(deleteUrl);
  }

  checkScheduleExists(
    path: string,
    fs: string,
    interval: number,
    frequency: RepeatFrequency,
    isSubvolume = false
  ): Observable<boolean> {
    const schedule = `${interval}${frequency}`;
    return this.getSnapshotScheduleList(path, fs, false).pipe(
      map((response) =>
        response.some(
          (item) =>
            this.schedulePathsMatch(item.path, path, isSubvolume) && item.schedule === schedule
        )
      ),
      catchError(() => {
        return of(false);
      })
    );
  }

  private normalizeSchedulePath(path: string): string {
    return (path ?? '').replace(/\/\.\.$/, '').replace(/\/+$/, '');
  }

  private schedulePathsMatch(schedulePath: string, targetPath: string, isSubvolume: boolean): boolean {
    const left = this.normalizeSchedulePath(schedulePath);
    const right = this.normalizeSchedulePath(targetPath);
    if (!left || !right) {
      return false;
    }
    if (isSubvolume) {
      return left.startsWith(right) || right.startsWith(left);
    }
    return left === right;
  }

  checkRetentionPolicyExists(
    path: string,
    fs: string,
    retentionFrequencies: string[],
    retentionFrequenciesRemoved: string[] = [],
    isSubvolume = false
  ): Observable<{ exists: boolean; errorIndex: number }> {
    return this.getSnapshotSchedule(path, fs, false).pipe(
      map((response) => {
        let errorIndex = -1;
        const matchingSchedules = response.filter((schedule) =>
          this.schedulePathsMatch(schedule.path, path, isSubvolume)
        );
        const existingFrequencies = new Set<string>();
        matchingSchedules.forEach((schedule) => {
          if (schedule.retention && typeof schedule.retention === 'object') {
            Object.keys(schedule.retention).forEach((frequency) =>
              existingFrequencies.add(frequency)
            );
          }
        });
        const result = retentionFrequencies?.length
          ? intersection(
              [...existingFrequencies].filter(
                (frequency) => !retentionFrequenciesRemoved.includes(frequency)
              ),
              retentionFrequencies
            )
          : [];
        const exists = !!result?.length;
        result?.forEach((frequency) => (errorIndex = retentionFrequencies.indexOf(frequency)));

        return { exists, errorIndex };
      }),
      catchError(() => {
        return of({ exists: false, errorIndex: -1 });
      })
    );
  }

  parseRetentionConflictFrequency(detail: string): string | null {
    const match = detail?.match(/Retention for (\S) is already present/i);
    return match ? match[1] : null;
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
                  ?.map?.(([frequency, interval]) => `${interval}${frequency}`)
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
