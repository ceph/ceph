import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/internal/Observable';
import { catchError, map } from 'rxjs/operators';
import { intersection, isEqual, uniqWith } from 'lodash';
import { SnapshotSchedule } from '../models/snapshot-schedule';
import { of } from 'rxjs';
import {
  RepeatFrequencyPlural,
  RepeatFrequencySingular
} from '../enum/repeat-frequency.enum';
import { RetentionFrequencyCopy } from '../enum/retention-frequency.enum';
import { DEFAULT_SUBVOLUME_GROUP } from '../constants/cephfs.constant';

@Injectable({
  providedIn: 'root'
})
export class CephfsSnapshotScheduleService {
  baseURL = 'api/cephfs';

  constructor(private http: HttpClient) {}

  normalizePath(path?: string): string {
    if (!path) {
      return '';
    }
    if (path === '/') {
      return '/';
    }
    return path.trim().replace(/\/+$/, '');
  }

  parseSubvolumePath(path: string): { group: string; subvol: string } | null {
    const parts = this.normalizePath(path).split('/').filter(Boolean);
    if (parts.length < 3 || parts[0] !== 'volumes') {
      return null;
    }
    return { group: parts[1], subvol: parts[2] };
  }

  isSubvolumePath(path: string): boolean {
    return !!this.parseSubvolumePath(path);
  }

  scheduleMatchesPath(schedule: SnapshotSchedule, targetPath: string): boolean {
    const normalized = this.normalizePath(targetPath);
    if (!normalized) {
      return false;
    }
    if (
      this.normalizePath(schedule.path) === normalized ||
      this.normalizePath(schedule.rel_path) === normalized
    ) {
      return true;
    }

    const subvolInfo = this.parseSubvolumePath(normalized);
    if (!subvolInfo?.subvol || !schedule.subvol) {
      return false;
    }
    if (schedule.subvol !== subvolInfo.subvol) {
      return false;
    }

    const scheduleGroup = schedule.group || DEFAULT_SUBVOLUME_GROUP;
    const targetGroup = subvolInfo.group || DEFAULT_SUBVOLUME_GROUP;
    return scheduleGroup === targetGroup;
  }

  appendSubvolumeParams(
    payload: Record<string, unknown>,
    path: string
  ): Record<string, unknown> {
    const subvolInfo = this.parseSubvolumePath(path);
    if (!subvolInfo) {
      return payload;
    }
    payload['subvol'] = subvolInfo.subvol;
    if (subvolInfo.group !== DEFAULT_SUBVOLUME_GROUP) {
      payload['group'] = subvolInfo.group;
    }
    return payload;
  }

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

  checkScheduleExists(path: string, fs: string, isSubvolume = false): Observable<boolean> {
    const normalizedPath = this.normalizePath(path);
    const queryPath =
      isSubvolume || this.isSubvolumePath(normalizedPath) ? '/' : normalizedPath || '/';
    return this.getSnapshotScheduleList(queryPath, fs, true).pipe(
      map((response) =>
        response.some((schedule) => this.scheduleMatchesPath(schedule, normalizedPath))
      ),
      catchError(() => of(false))
    );
  }

  isScheduleExistsError(error: { status?: number; error?: { detail?: string } }): boolean {
    const detail = error?.error?.detail ?? '';
    return error?.status === 409 || /EEXIST|existing schedule/i.test(detail);
  }

  checkRetentionPolicyExists(
    path: string,
    fs: string,
    retentionFrequencies: string[],
    retentionFrequenciesRemoved: string[] = [],
    isSubvolume = false
  ): Observable<{ exists: boolean; errorIndex: number }> {
    return this.getSnapshotSchedule(path, fs, this.isSubvolumePath(path) || isSubvolume).pipe(
      map((response) => {
        let errorIndex = -1;
        let exists = false;
        const index = response.findIndex((x) => this.scheduleMatchesPath(x, path));
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
        `${this.baseURL}/snapshot/schedule/${fs}?path=${encodeURIComponent(
          path
        )}&recursive=${recursive}`
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
    const match = schedule.match(/^(\d+)([a-zA-Z])$/);
    if (!match) return schedule;
    const interval = Number(match[1]);
    const unit = match[2];
    const label =
      interval > 1 ? RepeatFrequencyPlural[unit] : RepeatFrequencySingular[unit];
    return $localize`Every ${interval > 1 ? interval + ' ' : ''}${label}`;
  }

  parseRetentionCopy(retention: string | Record<string, number>): string[] {
    if (!retention) return ['-'];
    return Object.entries(retention).map(([frequency, interval]) =>
      $localize`${interval} ${RetentionFrequencyCopy[frequency]}`.toLocaleLowerCase()
    );
  }
}
