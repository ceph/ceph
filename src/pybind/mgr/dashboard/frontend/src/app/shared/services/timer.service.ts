import { Injectable } from '@angular/core';

import { Observable, timer } from 'rxjs';
import { observeOn, shareReplay, switchMap } from 'rxjs/operators';

import { whenPageVisible } from '../rxjs/operators/page-visibility.operator';
import { NgZoneSchedulerService } from './ngzone-scheduler.service';

@Injectable({
  providedIn: 'root'
})
export class TimerService {
  readonly DEFAULT_REFRESH_INTERVAL = 5000;
  readonly DEFAULT_DUE_TIME = 0;
  constructor(private ngZone: NgZoneSchedulerService) {}

  get(
    next: () => Observable<any>,
    refreshInterval: number = this.DEFAULT_REFRESH_INTERVAL,
    dueTime: number = this.DEFAULT_DUE_TIME
  ): Observable<any> {
    return timer(dueTime, refreshInterval, this.ngZone.leave).pipe(
      observeOn(this.ngZone.enter),
      switchMap(next),
      shareReplay({ refCount: true, bufferSize: 1 }),
      whenPageVisible()
    );
  }
}
