import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { BehaviorSubject, Observable, of, throwError } from 'rxjs';
import { mergeMap, take, tap } from 'rxjs/operators';

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
        const selectedDaemon = this.selectedDaemon.getValue();
        // Set or re-select the default daemon if the current one is not
        // in the list anymore.
        if (_.isEmpty(selectedDaemon) || undefined === _.find(daemons, { id: selectedDaemon.id })) {
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
        // If there is no selected daemon, retrieve daemon list so default daemon will be selected.
        _.isEmpty(daemon)
          ? this.list().pipe(
              mergeMap((daemons) =>
                _.isEmpty(daemons) ? throwError('No RGW daemons found!') : this.selectedDaemon$
              )
            )
          : of(daemon)
      ),
      take(1),
      mergeMap((daemon: RgwDaemon) => {
        let params = new HttpParams();
        params = params.append('daemon_name', daemon.id);
        return next(params);
      })
    );
  }

  setMultisiteConfig(realm_name: string, zonegroup_name: string, zone_name: string) {
    return this.request((params: HttpParams) => {
      params = params.appendAll({
        realm_name: realm_name,
        zonegroup_name: zonegroup_name,
        zone_name: zone_name
      });
      return this.http.put(`${this.url}/set_multisite_config`, null, { params: params });
    });
  }
}
