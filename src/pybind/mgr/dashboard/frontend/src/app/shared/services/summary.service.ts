import { HttpClient } from '@angular/common/http';
import { Injectable, NgZone } from '@angular/core';
import { Router } from '@angular/router';

import * as _ from 'lodash';
import { BehaviorSubject, Subscription } from 'rxjs';

import { ExecutingTask } from '../models/executing-task';
import { ServicesModule } from './services.module';

@Injectable({
  providedIn: ServicesModule
})
export class SummaryService {
  // Observable sources
  private summaryDataSource = new BehaviorSubject(null);

  // Observable streams
  summaryData$ = this.summaryDataSource.asObservable();

  constructor(private http: HttpClient, private router: Router, private ngZone: NgZone) {
    this.refresh();
  }

  refresh() {
    if (this.router.url !== '/login') {
      this.http.get('api/summary').subscribe((data) => {
        this.summaryDataSource.next(data);
      });
    }

    this.ngZone.runOutsideAngular(() => {
      setTimeout(() => {
        this.ngZone.run(() => {
          this.refresh();
        });
      }, 5000);
    });
  }

  /**
   * Returns the current value of summaryData
   *
   * @returns {object}
   * @memberof SummaryService
   */
  getCurrentSummary() {
    return this.summaryDataSource.getValue();
  }

  /**
   * Subscribes to the summaryData,
   * which is updated once every 5 seconds or when a new task is created.
   *
   * @param {(summary: any) => void} call
   * @param {(error: any) => void} error
   * @returns {Subscription}
   * @memberof SummaryService
   */
  subscribe(call: (summary: any) => void, error?: (error: any) => void): Subscription {
    return this.summaryData$.subscribe(call, error);
  }

  /**
   * Inserts a newly created task to the local list of executing tasks.
   * After that, it will automatically push that new information
   * to all subscribers.
   *
   * @param {ExecutingTask} task
   * @memberof SummaryService
   */
  addRunningTask(task: ExecutingTask) {
    const current = this.summaryDataSource.getValue();
    if (!current) {
      return;
    }

    if (_.isArray(current.executing_tasks)) {
      current.executing_tasks.push(task);
    } else {
      current.executing_tasks = [task];
    }

    this.summaryDataSource.next(current);
  }
}
