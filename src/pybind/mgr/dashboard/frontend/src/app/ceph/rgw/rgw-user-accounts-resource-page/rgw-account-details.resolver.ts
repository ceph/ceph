import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Resolve } from '@angular/router';
import { Observable, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { Account } from '../models/rgw-user-accounts';
import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';

@Injectable({
  providedIn: 'root'
})
export class RgwAccountDetailsResolver implements Resolve<Account | null> {
  constructor(private rgwUserAccountsService: RgwUserAccountsService) {}

  resolve(route: ActivatedRouteSnapshot): Observable<Account | null> {
    const accountName = route.paramMap.get('accountName') ?? '';
    if (!accountName) {
      return of(null);
    }

    return this.rgwUserAccountsService.list(true).pipe(
      map((accounts: Account[]) => {
        const account = accounts.find((item) => item.name === accountName);
        return account ?? null;
      }),
      catchError(() => of(null))
    );
  }
}
