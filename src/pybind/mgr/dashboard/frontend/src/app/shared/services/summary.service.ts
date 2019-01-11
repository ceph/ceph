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

  polling: number;

  constructor(private http: HttpClient, private router: Router, private ngZone: NgZone) {
    this.enablePolling();
  }

  enablePolling() {
    this.refresh();

    this.ngZone.runOutsideAngular(() => {
      this.polling = window.setInterval(() => {
        this.ngZone.run(() => {
          this.refresh();
        });
      }, 5000);
    });
  }

  refresh() {
    if (this.router.url !== '/login') {
      this.http.get('api/summary').subscribe((data) => {
        this.summaryDataSource.next(data);
      });
    }
  }

  /**
   * Returns the current value of summaryData
   */
  getCurrentSummary(): { [key: string]: any; executing_tasks: object[] } {
    return this.summaryDataSource.getValue();
  }

  /**
   * Subscribes to the summaryData,
   * which is updated once every 5 seconds or when a new task is created.
   */
  subscribe(next: (summary: any) => void, error?: (error: any) => void): Subscription {
    return this.summaryData$.subscribe(next, error);
  }

  /**
   * Inserts a newly created task to the local list of executing tasks.
   * After that, it will automatically push that new information
   * to all subscribers.
   */
  addRunningTask(task: ExecutingTask) {
    const current = this.summaryDataSource.getValue();
    if (!current) {
      return;
    }

    if (_.isArray(current.executing_tasks)) {
      const exists = current.executing_tasks.find((element) => {
        return element.name === task.name && _.isEqual(element.metadata, task.metadata);
      });
      if (!exists) {
        current.executing_tasks.push(task);
      }
    } else {
      current.executing_tasks = [task];
    }

    this.summaryDataSource.next(current);
  }
}
