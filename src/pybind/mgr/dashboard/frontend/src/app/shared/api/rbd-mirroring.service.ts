import { HttpClient } from '@angular/common/http';
import { Injectable, NgZone } from '@angular/core';

import { BehaviorSubject, Subscription } from 'rxjs';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class RbdMirroringService {
  // Observable sources
  private summaryDataSource = new BehaviorSubject(null);

  // Observable streams
  summaryData$ = this.summaryDataSource.asObservable();

  constructor(private http: HttpClient, private ngZone: NgZone) {
    this.refreshAndSchedule();
  }

  refresh() {
    this.http.get('api/rbdmirror').subscribe((data) => {
      this.summaryDataSource.next(data);
    });
  }

  refreshAndSchedule() {
    this.refresh();

    this.ngZone.runOutsideAngular(() => {
      setTimeout(() => {
        this.ngZone.run(() => {
          this.refreshAndSchedule();
        });
      }, 30000);
    });
  }

  /**
   * Returns the current value of summaryData
   */
  getCurrentSummary(): object {
    return this.summaryDataSource.getValue();
  }

  /**
   * Subscribes to the summaryData,
   * which is updated once every 5 seconds or when a new task is created.
   */
  subscribe(next: (summary: any) => void, error?: (error: any) => void): Subscription {
    return this.summaryData$.subscribe(next, error);
  }
}
