import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { BehaviorSubject, Observable, Subscription } from 'rxjs';

import { ExecutingTask } from '../models/executing-task';
import { TimerService } from './timer.service';

@Injectable({
  providedIn: 'root'
})
export class SummaryService {
  readonly REFRESH_INTERVAL = 5000;
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
    return this.http.get('api/summary');
  }

  private retrieveSummaryObserver(): (data: any) => void {
    return (data: Object) => {
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
      const exists = current.executing_tasks.find((element: any) => {
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
