import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/internal/Observable';
import { catchError, map } from 'rxjs/operators';
import { intersection, isEqual, uniqWith } from 'lodash';
import { SnapshotSchedule } from '../models/snapshot-schedule';
import { of } from 'rxjs';
import { RepeatFrequency } from '../enum/repeat-frequency.enum';

@Injectable({
  providedIn: 'root'
})
export class CephfsSnapshotScheduleService {
  baseURL = 'api/cephfs';

  constructor(private http: HttpClient) {}

  create(data: Record<string, any>): Observable<any> {
    return this.http.post(`${this.baseURL}/snapshot/schedule`, data, { observe: 'response' });
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
    retentionFrequencies: string[]
  ): Observable<{ exists: boolean; errorIndex: number }> {
    return this.getList(path, fs, false).pipe(
      map((response) => {
        let errorIndex = -1;
        let exists = false;
        const index = response.findIndex((x) => x.path === path);
        const result = retentionFrequencies?.length
          ? intersection(Object.keys(response?.[index]?.retention), retentionFrequencies)
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

  private getList(path: string, fs: string, recursive = true): Observable<SnapshotSchedule[]> {
    return this.http
      .get<SnapshotSchedule[]>(
        `${this.baseURL}/snapshot/schedule?path=${path}&fs=${fs}&recursive=${recursive}`
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
    return this.getList(path, fs, recursive).pipe(
      map((snapList: SnapshotSchedule[]) =>
        uniqWith(
          snapList.map((snapItem: SnapshotSchedule) => ({
            ...snapItem,
            status: snapItem.active ? 'Active' : 'Inactive',
            subvol: snapItem?.subvol || ' - ',
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
}
