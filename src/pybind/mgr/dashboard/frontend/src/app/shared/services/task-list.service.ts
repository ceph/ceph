import { Injectable, OnDestroy } from '@angular/core';

import { Observable, Subscription } from 'rxjs';

import { ExecutingTask } from '../models/executing-task';
import { SummaryService } from './summary.service';
import { TaskMessageService } from './task-message.service';

@Injectable()
export class TaskListService implements OnDestroy {
  summaryDataSubscription: Subscription;

  getUpdate: () => Observable<object>;
  preProcessing: (_: any) => any[];
  setList: (_: any[]) => void;
  onFetchError: (error: any) => void;
  taskFilter: (task: ExecutingTask) => boolean;
  itemFilter: (item: any, task: ExecutingTask) => boolean;
  builders: object;

  constructor(
    private taskMessageService: TaskMessageService,
    private summaryService: SummaryService
  ) {}

  /**
   * @param {() => Observable<object>} getUpdate Method that calls the api and
   * returns that without subscribing.
   * @param {(_: any) => any[]} preProcessing Method executed before merging
   * Tasks with Items
   * @param {(_: any[]) => void} setList  Method used to update array of item in the component.
   * @param {(error: any) => void} onFetchError Method called when there were
   * problems while fetching data.
   * @param {(task: ExecutingTask) => boolean} taskFilter callback used in tasks_array.filter()
   * @param {(item, task: ExecutingTask) => boolean} itemFilter callback used in
   * items_array.filter()
   * @param {object} builders
   * object with builders for each type of task.
   * You can also use a 'default' one.
   * @memberof TaskListService
   */
  init(
    getUpdate: () => Observable<object>,
    preProcessing: (_: any) => any[],
    setList: (_: any[]) => void,
    onFetchError: (error: any) => void,
    taskFilter: (task: ExecutingTask) => boolean,
    itemFilter: (item: any, task: ExecutingTask) => boolean,
    builders: object
  ) {
    this.getUpdate = getUpdate;
    this.preProcessing = preProcessing;
    this.setList = setList;
    this.onFetchError = onFetchError;
    this.taskFilter = taskFilter;
    this.itemFilter = itemFilter;
    this.builders = builders || {};

    this.summaryDataSubscription = this.summaryService.subscribe((summary) => {
      this.getUpdate().subscribe((resp: any) => {
        this.updateData(resp, summary['executing_tasks'].filter(this.taskFilter));
      }, this.onFetchError);
    }, this.onFetchError);
  }

  private updateData(resp: any, tasks: ExecutingTask[]) {
    const data: any[] = this.preProcessing ? this.preProcessing(resp) : resp;
    this.addMissing(data, tasks);
    data.forEach((item) => {
      const executingTasks = tasks.filter((task) => this.itemFilter(item, task));
      item.cdExecuting = this.getTaskAction(executingTasks);
    });
    this.setList(data);
  }

  private addMissing(data: any[], tasks: ExecutingTask[]) {
    const defaultBuilder = this.builders['default'];
    tasks.forEach((task) => {
      const existing = data.find((item) => this.itemFilter(item, task));
      const builder = this.builders[task.name];
      if (!existing && (builder || defaultBuilder)) {
        data.push(builder ? builder(task.metadata) : defaultBuilder(task.metadata));
      }
    });
  }

  private getTaskAction(tasks: ExecutingTask[]): string {
    if (tasks.length === 0) {
      return undefined;
    }
    return tasks
      .map((task) => {
        const progress = task.progress ? ` ${task.progress}%` : '';
        return this.taskMessageService.getRunningText(task) + '...' + progress;
      })
      .join(', ');
  }

  ngOnDestroy() {
    if (this.summaryDataSubscription) {
      this.summaryDataSubscription.unsubscribe();
    }
  }
}
