import { Injectable } from '@angular/core';

import { Observable, Subscriber } from 'rxjs';

import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import { ExecutingTask } from '../models/executing-task';
import { FinishedTask } from '../models/finished-task';
import { NotificationService } from './notification.service';
import { SummaryService } from './summary.service';
import { TaskManagerService } from './task-manager.service';
import { TaskMessageService } from './task-message.service';

@Injectable({
  providedIn: 'root'
})
export class TaskWrapperService {
  constructor(
    private notificationService: NotificationService,
    private summaryService: SummaryService,
    private taskMessageService: TaskMessageService,
    private taskManagerService: TaskManagerService
  ) {}

  wrapTaskAroundCall({ task, call }: { task: FinishedTask; call: Observable<any> }) {
    return new Observable((observer: Subscriber<any>) => {
      call.subscribe(
        (resp) => {
          if (resp.status === 202) {
            this._handleExecutingTasks(task);
          } else {
            this.summaryService.refresh();
            task.success = true;
            this.notificationService.notifyTask(task);
          }
        },
        (resp) => {
          task.success = false;
          task.exception = resp.error;
          observer.error(resp);
        },
        () => {
          observer.complete();
        }
      );
    });
  }

  _handleExecutingTasks(task: FinishedTask) {
    const notification = new CdNotificationConfig(
      NotificationType.info,
      this.taskMessageService.getRunningTitle(task)
    );
    notification.isFinishedTask = true;
    this.notificationService.show(notification);

    const executingTask = new ExecutingTask(task.name, task.metadata);
    this.summaryService.addRunningTask(executingTask);

    this.taskManagerService.subscribe(
      executingTask.name,
      executingTask.metadata,
      (asyncTask: FinishedTask) => {
        this.notificationService.notifyTask(asyncTask);
      }
    );
  }
}
