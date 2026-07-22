import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Resolve } from '@angular/router';
import { Observable, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

import _ from 'lodash';
import { ExtendedRgwUser, RgwUser } from '~/app/ceph/rgw/models/rgw-user';

import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';

@Injectable({
  providedIn: 'root'
})
export class RgwUserDetailsResolver implements Resolve<ExtendedRgwUser | null> {
  constructor(
    private rgwUserService: RgwUserService,
    private rgwUserAccountsService: RgwUserAccountsService
  ) {}

  resolve(route: ActivatedRouteSnapshot): Observable<ExtendedRgwUser | null> {
    const uid = route.paramMap.get('uid') ?? '';
    if (!uid) {
      return of(null);
    }

    return this.rgwUserService.get(uid).pipe(
      switchMap((user: RgwUser) =>
        this.rgwUserService.getQuota(uid).pipe(
          catchError(() => of({})),
          map((quotaResp: Partial<RgwUser>) => ({ user, quotaResp }))
        )
      ),
      switchMap(({ user, quotaResp }) =>
        this.rgwUserService.getUserRateLimit(uid).pipe(
          catchError(() => of({})),
          map((rateLimitResp: Record<string, number | boolean>) => ({
            user,
            quotaResp,
            rateLimitResp
          }))
        )
      ),
      switchMap(({ user, quotaResp, rateLimitResp }) => {
        if (!user?.account_id) {
          return of({
            ...user,
            ...quotaResp,
            ...rateLimitResp,
            subusers: _.sortBy(user?.subusers, 'id'),
            caps: _.sortBy(user?.caps, 'type')
          });
        }

        return this.rgwUserAccountsService.get(user.account_id).pipe(
          catchError(() => of(null)),
          map((account: any) => ({
            ...user,
            ...quotaResp,
            ...rateLimitResp,
            account: account ?? undefined,
            subusers: _.sortBy(user?.subusers, 'id'),
            caps: _.sortBy(user?.caps, 'type')
          }))
        );
      }),
      catchError(() => of(null))
    );
  }
}
