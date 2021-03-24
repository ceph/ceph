import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { BehaviorSubject, Observable, of, throwError } from 'rxjs';
import { mergeMap, retryWhen, take, tap } from 'rxjs/operators';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { cdEncode } from '~/app/shared/decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class RgwDaemonService {
  private url = 'api/rgw/daemon';
  private daemons = new BehaviorSubject<RgwDaemon[]>([]);
  daemons$ = this.daemons.asObservable();
  private selectedDaemon = new BehaviorSubject<RgwDaemon>(null);
  selectedDaemon$ = this.selectedDaemon.asObservable();

  constructor(private http: HttpClient) {}

  list(): Observable<RgwDaemon[]> {
    return this.http.get<RgwDaemon[]>(this.url).pipe(
      tap((daemons: RgwDaemon[]) => {
        this.daemons.next(daemons);
        if (_.isEmpty(this.selectedDaemon.getValue())) {
          this.selectDefaultDaemon(daemons);
        }
      })
    );
  }

  get(id: string) {
    return this.http.get(`${this.url}/${id}`);
  }

  selectDaemon(daemon: RgwDaemon) {
    this.selectedDaemon.next(daemon);
  }

  private selectDefaultDaemon(daemons: RgwDaemon[]): RgwDaemon {
    if (daemons.length === 0) {
      return null;
    }

    for (const daemon of daemons) {
      if (daemon.default) {
        this.selectDaemon(daemon);
        return daemon;
      }
    }

    this.selectDaemon(daemons[0]);
    return daemons[0];
  }

  request(next: (params: HttpParams) => Observable<any>) {
    return this.selectedDaemon.pipe(
      mergeMap((daemon: RgwDaemon) =>
        // If there is no selected daemon, retrieve daemon list (default daemon will be selected)
        // and try again if daemon list is not empty.
        _.isEmpty(daemon)
          ? this.list().pipe(mergeMap((daemons) => throwError(!_.isEmpty(daemons))))
          : of(daemon)
      ),
      retryWhen((error) =>
        error.pipe(
          mergeMap((hasToRetry) => (hasToRetry ? error : throwError('No RGW daemons found!')))
        )
      ),
      take(1),
      mergeMap((daemon: RgwDaemon) => {
        let params = new HttpParams();
        params = params.append('daemon_name', daemon.id);
        return next(params);
      })
    );
  }
}
