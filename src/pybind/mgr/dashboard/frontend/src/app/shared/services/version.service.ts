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
export class VersionService {
  // Observable sources
  private versionDataSource = new BehaviorSubject(null);

  // Observable streams
  versionData$ = this.versionDataSource.asObservable();

  constructor(private http: HttpClient, private router: Router, private ngZone: NgZone) {
    this.refresh();
  }

  refresh() {
    this.http.get('api/public/version').subscribe((data) => {
      this.versionDataSource.next(data);
    });

    this.ngZone.runOutsideAngular(() => {
      setTimeout(() => {
        this.ngZone.run(() => {
          this.refresh();
        });
      }, 5000);
    });
  }

  /**
   * Returns the current value of versionData
   *
   * @returns {object}
   * @memberof VersionService
   */
  getCurrentVersion() {
    return this.versionDataSource.getValue();
  }

  /**
   * Subscribes to the versionData,
   * which is updated once every 5 seconds or when a new task is created.
   *
   * @param {(version: any) => void} call
   * @param {(error: any) => void} error
   * @returns {Subscription}
   * @memberof VersionService
   */
  subscribe(call: (version: any) => void, error?: (error: any) => void): Subscription {
    return this.versionData$.subscribe(call, error);
  }

  /**
   * Inserts a newly created task to the local list of executing tasks.
   * After that, it will automatically push that new information
   * to all subscribers.
   *
   * @param {ExecutingTask} task
   * @memberof VersionService
   */
  addRunningTask(task: ExecutingTask) {
    const current = this.versionDataSource.getValue();
    if (!current) {
      return;
    }

    if (_.isArray(current.executing_tasks)) {
      current.executing_tasks.push(task);
    } else {
      current.executing_tasks = [task];
    }

    this.versionDataSource.next(current);
  }
}
