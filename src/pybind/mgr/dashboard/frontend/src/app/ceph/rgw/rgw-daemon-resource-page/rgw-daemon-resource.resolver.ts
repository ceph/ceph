import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Resolve } from '@angular/router';
import { Observable, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';

@Injectable({
  providedIn: 'root'
})
export class RgwDaemonResourceResolver implements Resolve<RgwDaemon | null> {
  constructor(private rgwDaemonService: RgwDaemonService) {}

  resolve(route: ActivatedRouteSnapshot): Observable<RgwDaemon | null> {
    const daemonId = route.paramMap.get('daemonId') ?? '';
    if (!daemonId) {
      return of(null);
    }

    return this.rgwDaemonService.list().pipe(
      map((daemons: RgwDaemon[]) => daemons.find((daemon) => daemon.id === daemonId) ?? null),
      catchError(() => of(null))
    );
  }
}
