import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { ExecutingTask } from '../models/executing-task';
import { FinishedTask } from '../models/finished-task';
import { Task } from '../models/task';
import { SummaryService } from './summary.service';

class TaskSubscription {
  name: string;
  metadata: object;
  onTaskFinished: (finishedTask: FinishedTask) => any;

  constructor(name, metadata, onTaskFinished) {
    this.name = name;
    this.metadata = metadata;
    this.onTaskFinished = onTaskFinished;
  }
}

@Injectable()
export class TaskManagerService {

  subscriptions: Array<TaskSubscription> = [];

  constructor(private summaryService: SummaryService) {
    summaryService.summaryData$.subscribe((data: any) => {
      const executingTasks = data.executing_tasks;
      const finishedTasks = data.finished_tasks;
      const newSubscriptions: Array<TaskSubscription> = [];
      for (const subscription of this.subscriptions) {
        const finishedTask = <FinishedTask>this._getTask(subscription, finishedTasks);
        const executingTask = <ExecutingTask>this._getTask(subscription, executingTasks);
        if (finishedTask !== null && executingTask === null) {
          subscription.onTaskFinished(finishedTask);
        }
        if (executingTask !== null) {
          newSubscriptions.push(subscription);
        }
        this.subscriptions = newSubscriptions;
      }
    });
  }

  subscribe(name, metadata, onTaskFinished: (finishedTask: FinishedTask) => any) {
    this.subscriptions.push(new TaskSubscription(name, metadata, onTaskFinished));
  }

  _getTask(subscription: TaskSubscription, tasks: Array<Task>): Task {
    for (const task of tasks) {
      if (task.name === subscription.name &&
        _.isEqual(task.metadata, subscription.metadata)) {
        return task;
      }
    }
    return null;
  }
}
