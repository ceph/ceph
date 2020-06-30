import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { BehaviorSubject, Observable, Subscription } from 'rxjs';
import { filter, first } from 'rxjs/operators';

import { ExecutingTask } from '../models/executing-task';
import { Summary } from '../models/summary.model';
import { TimerService } from './timer.service';

@Injectable({
  providedIn: 'root'
})
export class SummaryService {
  readonly REFRESH_INTERVAL = 5000;
  // Observable sources
  private summaryDataSource = new BehaviorSubject<Summary>(null);
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

  private retrieveSummaryObservable(): Observable<Summary> {
    return this.http.get<Summary>('api/summary');
  }

  private retrieveSummaryObserver(): (data: Summary) => void {
    return (data: Summary) => {
      this.summaryDataSource.next(data);
    };
  }

  /**
   * Subscribes to the summaryData and receive only the first, non undefined, value.
   */
  subscribeOnce(next: (summary: Summary) => void, error?: (error: any) => void): Subscription {
    return this.summaryData$
      .pipe(
        filter((value) => !!value),
        first()
      )
      .subscribe(next, error);
  }

  /**
   * Subscribes to the summaryData,
   * which is updated periodically or when a new task is created.
   * Will receive only non undefined values.
   */
  subscribe(next: (summary: Summary) => void, error?: (error: any) => void): Subscription {
    return this.summaryData$.pipe(filter((value) => !!value)).subscribe(next, error);
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
