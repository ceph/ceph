import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';
import { Subscriber } from 'rxjs/Subscriber';

import { NotificationType } from '../enum/notification-type.enum';
import { ExecutingTask } from '../models/executing-task';
import { FinishedTask } from '../models/finished-task';
import { NotificationService } from './notification.service';
import { ServicesModule } from './services.module';
import { TaskManagerMessageService } from './task-manager-message.service';
import { TaskManagerService } from './task-manager.service';

@Injectable({
  providedIn: ServicesModule
})
export class TaskWrapperService {
  constructor(
    private notificationService: NotificationService,
    private taskManagerMessageService: TaskManagerMessageService,
    private taskManagerService: TaskManagerService
  ) {}

  wrapTaskAroundCall({
    task,
    call,
    tasks
  }: {
    task: FinishedTask;
    call: Observable<any>;
    tasks?: ExecutingTask[];
  }) {
    return new Observable((observer: Subscriber<any>) => {
      call.subscribe(
        (resp) => {
          if (resp.status === 202) {
            this._handleExecutingTasks(task, tasks);
          } else {
            task.success = true;
            this.notificationService.notifyTask(task);
          }
        },
        (resp) => {
          task.success = false;
          task.exception = resp.error;
          this.notificationService.notifyTask(task);
          observer.error();
        },
        () => {
          observer.complete();
        }
      );
    });
  }

  _handleExecutingTasks(task: FinishedTask, tasks?: ExecutingTask[]) {
    this.notificationService.show(
      NotificationType.info,
      task.name + ' in progress...',
      this.taskManagerMessageService.getDescription(task)
    );
    const executingTask = new ExecutingTask(task.name, task.metadata);
    if (tasks) {
      tasks.push(executingTask);
    }
    this.taskManagerService.subscribe(
      executingTask.name,
      executingTask.metadata,
      (asyncTask: FinishedTask) => {
        this.notificationService.notifyTask(asyncTask);
      }
    );
  }
}
