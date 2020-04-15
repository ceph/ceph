import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { BehaviorSubject, Observable, Subscription } from 'rxjs';

import { cdEncode, cdEncodeNot } from '../decorators/cd-encode';
import { TimerService } from '../services/timer.service';
import { ApiModule } from './api.module';

@cdEncode
@Injectable({
  providedIn: ApiModule
})
export class RbdMirroringService {
  readonly REFRESH_INTERVAL = 30000;
  // Observable sources
  private summaryDataSource = new BehaviorSubject(null);
  // Observable streams
  summaryData$ = this.summaryDataSource.asObservable();

  constructor(private http: HttpClient, private timerService: TimerService) {}

  startPolling(): Subscription {
    return this.timerService
      .get(() => this.retrieveSummaryObservable(), this.REFRESH_INTERVAL)
      .subscribe(this.retrieveSummaryObserver());
  }

  refresh(): Subscription {
    return this.retrieveSummaryObservable().subscribe(this.retrieveSummaryObserver());
  }

  private retrieveSummaryObservable(): Observable<Object> {
    return this.http.get('api/block/mirroring/summary');
  }

  private retrieveSummaryObserver(): (data: any) => void {
    return (data: any) => {
      this.summaryDataSource.next(data);
    };
  }

  /**
   * Returns the current value of summaryData
   */
  getCurrentSummary(): { [key: string]: any; executing_tasks: object[] } {
    return this.summaryDataSource.getValue();
  }

  /**
   * Subscribes to the summaryData,
   * which is updated periodically or when a new task is created.
   */
  subscribeSummary(next: (summary: any) => void, error?: (error: any) => void): Subscription {
    return this.summaryData$.subscribe(next, error);
  }

  getPool(poolName: string) {
    return this.http.get(`api/block/mirroring/pool/${poolName}`);
  }

  updatePool(poolName: string, request: any) {
    return this.http.put(`api/block/mirroring/pool/${poolName}`, request, { observe: 'response' });
  }

  getSiteName() {
    return this.http.get(`api/block/mirroring/site_name`);
  }

  setSiteName(@cdEncodeNot siteName: string) {
    return this.http.put(
      `api/block/mirroring/site_name`,
      { site_name: siteName },
      { observe: 'response' }
    );
  }

  createBootstrapToken(poolName: string) {
    return this.http.post(`api/block/mirroring/pool/${poolName}/bootstrap/token`, {});
  }

  importBootstrapToken(
    poolName: string,
    @cdEncodeNot direction: string,
    @cdEncodeNot token: string
  ) {
    const request = {
      direction: direction,
      token: token
    };
    return this.http.post(`api/block/mirroring/pool/${poolName}/bootstrap/peer`, request, {
      observe: 'response'
    });
  }

  getPeer(poolName: string, peerUUID: string) {
    return this.http.get(`api/block/mirroring/pool/${poolName}/peer/${peerUUID}`);
  }

  addPeer(poolName: string, request: any) {
    return this.http.post(`api/block/mirroring/pool/${poolName}/peer`, request, {
      observe: 'response'
    });
  }

  updatePeer(poolName: string, peerUUID: string, request: any) {
    return this.http.put(`api/block/mirroring/pool/${poolName}/peer/${peerUUID}`, request, {
      observe: 'response'
    });
  }

  deletePeer(poolName: string, peerUUID: string) {
    return this.http.delete(`api/block/mirroring/pool/${poolName}/peer/${peerUUID}`, {
      observe: 'response'
    });
  }
}
