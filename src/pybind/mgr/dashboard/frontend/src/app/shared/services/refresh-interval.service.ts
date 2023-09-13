import { Injectable, NgZone, OnDestroy } from '@angular/core';

import { BehaviorSubject, interval, Subscription } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class RefreshIntervalService implements OnDestroy {
  private intervalTime: number;
  // Observable sources
  private intervalDataSource = new BehaviorSubject(null);
  private intervalSubscription: Subscription;
  // Observable streams
  intervalData$ = this.intervalDataSource.asObservable();

  constructor(private ngZone: NgZone) {
    const initialInterval = parseInt(sessionStorage.getItem('dashboard_interval'), 10) || 5000;
    this.setRefreshInterval(initialInterval);
  }

  setRefreshInterval(newInterval: number) {
    this.intervalTime = newInterval;
    sessionStorage.setItem('dashboard_interval', newInterval.toString());

    if (this.intervalSubscription) {
      this.intervalSubscription.unsubscribe();
    }
    this.ngZone.runOutsideAngular(() => {
      this.intervalSubscription = interval(this.intervalTime).subscribe(() =>
        this.ngZone.run(() => {
          this.intervalDataSource.next(this.intervalTime);
        })
      );
    });
  }

  getRefreshInterval() {
    return this.intervalTime;
  }

  ngOnDestroy() {
    if (this.intervalSubscription) {
      this.intervalSubscription.unsubscribe();
    }
  }
}
