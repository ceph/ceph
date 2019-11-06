import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  NgZone,
  OnDestroy,
  OnInit
} from '@angular/core';

import { Mutex } from 'async-mutex';
import * as _ from 'lodash';
import * as moment from 'moment';
import { LocalStorage } from 'ngx-store';

import { ExecutingTask } from '../../../shared/models/executing-task';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskMessageService } from '../../../shared/services/task-message.service';
import { Icons } from '../../enum/icons.enum';
import { CdNotification } from '../../models/cd-notification';
import { FinishedTask } from '../../models/finished-task';
import { AuthStorageService } from '../../services/auth-storage.service';
import { NotificationService } from '../../services/notification.service';
import { PrometheusAlertService } from '../../services/prometheus-alert.service';
import { PrometheusNotificationService } from '../../services/prometheus-notification.service';

@Component({
  selector: 'cd-notifications-sidebar',
  templateUrl: './notifications-sidebar.component.html',
  styleUrls: ['./notifications-sidebar.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class NotificationsSidebarComponent implements OnInit, OnDestroy {
  notifications: CdNotification[];
  private interval: number;

  executingTasks: ExecutingTask[] = [];

  icons = Icons;

  // Tasks
  @LocalStorage() last_task = '';
  mutex = new Mutex();

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
  }

  ngOnInit() {
    if (this.authStorageService.getPermissions().prometheus.read) {
      this.triggerPrometheusAlerts();
      this.ngZone.runOutsideAngular(() => {
        this.interval = window.setInterval(() => {
          this.ngZone.run(() => {
            this.triggerPrometheusAlerts();
          });
        }, 5000);
      });
    }

    this.notificationService.data$.subscribe((notifications: CdNotification[]) => {
      this.notifications = _.orderBy(notifications, ['timestamp'], ['desc']);
      this.cdRef.detectChanges();
    });

    this.summaryService.subscribe((data: any) => {
      if (!data) {
        return;
      }
      this._handleTasks(data.executing_tasks);

      this.mutex.acquire().then((release) => {
        _.filter(
          data.finished_tasks,
          (task: FinishedTask) => !this.last_task || moment(task.end_time).isAfter(this.last_task)
        ).forEach((task) => {
          const config = this.notificationService.finishedTaskToNotification(task, task.success);
          const notification = new CdNotification(config);
          notification.timestamp = task.end_time;
          notification.duration = task.duration;

          if (!this.last_task || moment(task.end_time).isAfter(this.last_task)) {
            this.last_task = task.end_time;
          }

          this.notificationService.save(notification);
        });

        this.cdRef.detectChanges();

        release();
      });
    });
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
    this.notificationService.toggleSidebar(true);
  }

  trackByFn(index) {
    return index;
  }
}
