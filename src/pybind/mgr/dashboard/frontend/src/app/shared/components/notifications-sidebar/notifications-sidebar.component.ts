import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  HostBinding,
  NgZone,
  OnDestroy,
  OnInit
} from '@angular/core';

import { Mutex } from 'async-mutex';
import * as _ from 'lodash';
import * as moment from 'moment';
import { Subscription } from 'rxjs';

import { Icons } from '../../enum/icons.enum';
import { CdNotification } from '../../models/cd-notification';
import { ExecutingTask } from '../../models/executing-task';
import { FinishedTask } from '../../models/finished-task';
import { AuthStorageService } from '../../services/auth-storage.service';
import { NotificationService } from '../../services/notification.service';
import { PrometheusAlertService } from '../../services/prometheus-alert.service';
import { PrometheusNotificationService } from '../../services/prometheus-notification.service';
import { SummaryService } from '../../services/summary.service';
import { TaskMessageService } from '../../services/task-message.service';

@Component({
  selector: 'cd-notifications-sidebar',
  templateUrl: './notifications-sidebar.component.html',
  styleUrls: ['./notifications-sidebar.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class NotificationsSidebarComponent implements OnInit, OnDestroy {
  @HostBinding('class.active') isSidebarOpened = false;

  notifications: CdNotification[];
  private interval: number;
  private timeout: number;

  executingTasks: ExecutingTask[] = [];

  private subs = new Subscription();

  icons = Icons;

  // Tasks
  last_task = '';
  mutex = new Mutex();

  simplebar = {
    autoHide: false
  };

  constructor(
    public notificationService: NotificationService,
    private summaryService: SummaryService,
    private taskMessageService: TaskMessageService,
    private prometheusNotificationService: PrometheusNotificationService,
    private authStorageService: AuthStorageService,
    private prometheusAlertService: PrometheusAlertService,
    private ngZone: NgZone,
    private cdRef: ChangeDetectorRef
  ) {
    this.notifications = [];
  }

  ngOnDestroy() {
    window.clearInterval(this.interval);
    window.clearTimeout(this.timeout);
    this.subs.unsubscribe();
  }

  ngOnInit() {
    this.last_task = window.localStorage.getItem('last_task');

    const permissions = this.authStorageService.getPermissions();
    if (permissions.prometheus.read && permissions.configOpt.read) {
      this.triggerPrometheusAlerts();
      this.ngZone.runOutsideAngular(() => {
        this.interval = window.setInterval(() => {
          this.ngZone.run(() => {
            this.triggerPrometheusAlerts();
          });
        }, 5000);
      });
    }

    this.subs.add(
      this.notificationService.data$.subscribe((notifications: CdNotification[]) => {
        this.notifications = _.orderBy(notifications, ['timestamp'], ['desc']);
        this.cdRef.detectChanges();
      })
    );

    this.subs.add(
      this.notificationService.sidebarSubject.subscribe((forceClose) => {
        if (forceClose) {
          this.isSidebarOpened = false;
        } else {
          this.isSidebarOpened = !this.isSidebarOpened;
        }

        window.clearTimeout(this.timeout);
        this.timeout = window.setTimeout(() => {
          this.cdRef.detectChanges();
        }, 0);
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
    for (const excutingTask of executingTasks) {
      excutingTask.description = this.taskMessageService.getRunningTitle(excutingTask);
    }
    this.executingTasks = executingTasks;
  }

  private triggerPrometheusAlerts() {
    this.prometheusAlertService.refresh();
    this.prometheusNotificationService.refresh();
  }

  removeAll() {
    this.notificationService.removeAll();
  }

  remove(index: number) {
    this.notificationService.remove(index);
  }

  closeSidebar() {
    this.isSidebarOpened = false;
  }

  trackByFn(index: number) {
    return index;
  }
}
