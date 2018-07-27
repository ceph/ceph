import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';
import { Subscriber } from 'rxjs/Subscriber';

import { NotificationType } from '../enum/notification-type.enum';
import { ExecutingTask } from '../models/executing-task';
import { FinishedTask } from '../models/finished-task';
import { NotificationService } from './notification.service';
import { ServicesModule } from './services.module';
import { SummaryService } from './summary.service';
import { TaskManagerMessageService } from './task-manager-message.service';
import { TaskManagerService } from './task-manager.service';

@Injectable({
  providedIn: ServicesModule
})
export class TaskWrapperService {
  constructor(
    private notificationService: NotificationService,
    private summaryService: SummaryService,
    private taskManagerMessageService: TaskManagerMessageService,
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
          observer.error();
        },
        () => {
          observer.complete();
        }
      );
    });
  }

  _handleExecutingTasks(task: FinishedTask) {
    this.notificationService.show(
      NotificationType.info,
      this.taskManagerMessageService.getRunningMessage(task),
      this.taskManagerMessageService.getDescription(task)
    );

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
