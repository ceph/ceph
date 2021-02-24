import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable, ReplaySubject } from 'rxjs';
import { mergeMap, take, tap } from 'rxjs/operators';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { cdEncode } from '~/app/shared/decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class RgwDaemonService {
  private url = 'api/rgw/daemon';
  private daemons = new ReplaySubject<RgwDaemon[]>(1);
  daemons$ = this.daemons.asObservable();
  private selectedDaemon = new ReplaySubject<RgwDaemon>(1);
  selectedDaemon$ = this.selectedDaemon.asObservable();

  constructor(private http: HttpClient) {}

  list(): Observable<RgwDaemon[]> {
    return this.http.get<RgwDaemon[]>(this.url).pipe(
      tap((daemons: RgwDaemon[]) => {
        this.daemons.next(daemons);
      })
    );
  }

  get(id: string) {
    return this.http.get(`${this.url}/${id}`);
  }

  selectDaemon(daemon: RgwDaemon) {
    this.selectedDaemon.next(daemon);
  }

  selectDefaultDaemon(daemons: RgwDaemon[]): RgwDaemon {
    for (const daemon of daemons) {
      if (daemon.default) {
        this.selectDaemon(daemon);
        return daemon;
      }
    }

    throw new Error('No default RGW daemon found.');
  }

  request(next: (params: HttpParams) => Observable<any>) {
    return this.selectedDaemon.pipe(
      take(1),
      mergeMap((daemon: RgwDaemon) => {
        let params = new HttpParams();
        params = params.append('daemon_name', daemon.id);
        return next(params);
      })
    );
  }
}
