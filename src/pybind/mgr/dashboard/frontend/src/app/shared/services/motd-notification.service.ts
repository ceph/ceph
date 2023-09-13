import { Injectable, OnDestroy } from '@angular/core';

import * as _ from 'lodash';
import { BehaviorSubject, EMPTY, Observable, of, Subscription } from 'rxjs';
import { catchError, delay, mergeMap, repeat, tap } from 'rxjs/operators';

import { Motd, MotdService } from '~/app/shared/api/motd.service';
import { whenPageVisible } from '../rxjs/operators/page-visibility.operator';

@Injectable({
  providedIn: 'root'
})
export class MotdNotificationService implements OnDestroy {
  public motd$: Observable<Motd | null>;
  public motdSource = new BehaviorSubject<Motd | null>(null);

  private subscription: Subscription;
  private localStorageKey = 'dashboard_motd_hidden';

  constructor(private motdService: MotdService) {
    this.motd$ = this.motdSource.asObservable();
    // Check every 60 seconds for the latest MOTD configuration.
    this.subscription = of(true)
      .pipe(
        mergeMap(() => this.motdService.get()),
        catchError((error) => {
          // Do not show an error notification.
          if (_.isFunction(error.preventDefault)) {
            error.preventDefault();
          }
          return EMPTY;
        }),
        tap((motd: Motd | null) => this.processResponse(motd)),
        delay(60000),
        repeat(),
        whenPageVisible()
      )
      .subscribe();
  }

  ngOnDestroy(): void {
    this.subscription.unsubscribe();
  }

  hide() {
    // Store the severity and MD5 of the current MOTD in local or
    // session storage to be able to show it again if the severity
    // or message of the latest MOTD has changed.
    const motd: Motd = this.motdSource.getValue();
    if (motd) {
      const value = `${motd.severity}:${motd.md5}`;
      switch (motd.severity) {
        case 'info':
          localStorage.setItem(this.localStorageKey, value);
          sessionStorage.removeItem(this.localStorageKey);
          break;
        case 'warning':
          sessionStorage.setItem(this.localStorageKey, value);
          localStorage.removeItem(this.localStorageKey);
          break;
      }
    }
    this.motdSource.next(null);
  }

  processResponse(motd: Motd | null) {
    const value: string | null =
      sessionStorage.getItem(this.localStorageKey) || localStorage.getItem(this.localStorageKey);
    let visible: boolean = _.isNull(value);
    // Force a hidden MOTD to be shown again if the severity or message
    // has been changed.
    if (!visible && motd) {
      const [severity, md5] = value.split(':');
      if (severity !== motd.severity || md5 !== motd.md5) {
        visible = true;
        sessionStorage.removeItem(this.localStorageKey);
        localStorage.removeItem(this.localStorageKey);
      }
    }
    if (visible) {
      this.motdSource.next(motd);
    }
  }
}
