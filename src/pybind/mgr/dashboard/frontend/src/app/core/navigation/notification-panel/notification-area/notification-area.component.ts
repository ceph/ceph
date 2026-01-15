import { Component, OnInit, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { Subscription } from 'rxjs';
import { NotificationService } from '../../../../shared/services/notification.service';
import { CdNotification } from '../../../../shared/models/cd-notification';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { SummaryService } from '~/app/shared/services/summary.service';
import { Mutex } from 'async-mutex';
import _ from 'lodash';
import { FinishedTask } from '~/app/shared/models/finished-task';
import moment from 'moment';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { TaskMessageService } from '~/app/shared/services/task-message.service';
import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-notification-area',
  templateUrl: './notification-area.component.html',
  styleUrls: ['./notification-area.component.scss'],
  standalone: false
})
export class NotificationAreaComponent implements OnInit, OnDestroy {
  todayNotifications: CdNotification[] = [];
  previousNotifications: CdNotification[] = [];
  private subs = new Subscription();
  last_task = '';
  mutex = new Mutex();
  icons = Icons;
  executingTasks: ExecutingTask[] = [];

  readonly notificationIconMap = {
    [NotificationType.success]: 'success',
    [NotificationType.error]: 'error',
    [NotificationType.info]: 'infoCircle',
    [NotificationType.warning]: 'warning',
    default: 'infoCircle'
  } as const;

  constructor(
    private notificationService: NotificationService,
    private summaryService: SummaryService,
    private cdRef: ChangeDetectorRef,
    private taskMessageService: TaskMessageService
  ) {}

  ngOnInit(): void {
    this.subs.add(
      this.notificationService.data$.subscribe((notifications: CdNotification[]) => {
        const today: Date = new Date();
        this.todayNotifications = [];
        this.previousNotifications = [];
        notifications.forEach((n: CdNotification) => {
          const notifDate = new Date(n.timestamp);
          if (
            notifDate.getDate() === today.getDate() &&
            notifDate.getMonth() === today.getMonth() &&
            notifDate.getFullYear() === today.getFullYear()
          ) {
            this.todayNotifications.push(n);
          } else {
            this.previousNotifications.push(n);
          }
        });
      })
    );

    this.subs.add(
      this.summaryService.subscribe((summary) => {
        this._handleTasks(summary.executing_tasks);

        this.mutex.acquire().then((release) => {
          _.filter(
            summary.finished_tasks,
            (task: FinishedTask) => !this.last_task || moment(task.end_time).isAfter(this.last_task)
          ).forEach((task) => {
            const config = this.notificationService.finishedTaskToNotification(task, task.success);
            const notification = new CdNotification(config);
            notification.timestamp = task.end_time;
            notification.duration = task.duration;

            if (!this.last_task || moment(task.end_time).isAfter(this.last_task)) {
              this.last_task = task.end_time;
              window.localStorage.setItem('last_task', this.last_task);
            }

            this.notificationService.save(notification);
          });

          this.cdRef.detectChanges();

          release();
        });
      })
    );
  }

  _handleTasks(executingTasks: ExecutingTask[]) {
    for (const executingTask of executingTasks) {
      executingTask.description = this.taskMessageService.getRunningTitle(executingTask);
    }
    this.executingTasks = executingTasks;
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  removeNotification(notification: CdNotification, event: MouseEvent) {
    // Stop event propagation to prevent panel closing
    event.stopPropagation();
    event.preventDefault();

    // Get the notification index from the service's data
    const notifications = this.notificationService['dataSource'].getValue();
    const index = notifications.findIndex(
      (n) => n.timestamp === notification.timestamp && n.title === notification.title
    );

    if (index > -1) {
      // Remove the notification through the service
      this.notificationService.remove(index);
    }
  }
}
