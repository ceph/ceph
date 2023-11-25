import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/internal/Observable';
import { SnapshotSchedule } from '../models/snapshot-schedule';
import { map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class CephfsSnapshotScheduleService {
  baseURL = 'api/cephfs';

  constructor(private http: HttpClient) {}

  getSnapshotScheduleList(
    path: string,
    fs: string,
    recursive = true
  ): Observable<SnapshotSchedule[]> {
    return this.http
      .get<SnapshotSchedule[]>(
        `${this.baseURL}/snaphost/schedule?path=${path}&fs=${fs}&recursive=${recursive}`
      )
      .pipe(
        map((snapList: SnapshotSchedule[]) =>
          snapList.map((snapItem: SnapshotSchedule) => ({
            ...snapItem,
            status: snapItem.active ? 'Active' : 'Inactive',
            subvol: snapItem?.subvol || ' - ',
            retention: Object.values(snapItem.retention)?.length
              ? Object.entries(snapItem.retention)
                  ?.map?.(([frequency, interval]) => `${interval}${frequency.toLocaleUpperCase()}`)
                  .join(' ')
              : '-'
          }))
        )
      );
  }
}
