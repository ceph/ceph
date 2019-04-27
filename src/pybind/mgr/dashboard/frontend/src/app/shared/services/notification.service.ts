import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { ToastOptions, ToastsManager } from 'ng2-toastr';
import { BehaviorSubject } from 'rxjs';

import { NotificationType } from '../enum/notification-type.enum';
import { CdNotification, CdNotificationConfig } from '../models/cd-notification';
import { FinishedTask } from '../models/finished-task';
import { CdDatePipe } from '../pipes/cd-date.pipe';
import { ServicesModule } from './services.module';
import { TaskMessageService } from './task-message.service';

@Injectable({
  providedIn: ServicesModule
})
export class NotificationService {
  private hideToasties = false;

  // Observable sources
  private dataSource = new BehaviorSubject<CdNotification[]>([]);
  private queuedNotifications: CdNotificationConfig[] = [];

  // Observable streams
  data$ = this.dataSource.asObservable();

  private queueTimeoutId: number;
  KEY = 'cdNotifications';

  constructor(
    public toastr: ToastsManager,
    private taskMessageService: TaskMessageService,
    private cdDatePipe: CdDatePipe
  ) {
    const stringNotifications = localStorage.getItem(this.KEY);
    let notifications: CdNotification[] = [];

    if (_.isString(stringNotifications)) {
      notifications = JSON.parse(stringNotifications, (_key, value) => {
        if (_.isPlainObject(value)) {
          return _.assign(new CdNotification(), value);
        }
        return value;
      });
    }

    this.dataSource.next(notifications);
  }

  /**
   * Removes all current saved notifications
   */
  removeAll() {
    localStorage.removeItem(this.KEY);
    this.dataSource.next([]);
  }

  /**
   * Method used for saving a shown notification (check show() method).
   */
  save(notification: CdNotification) {
    const recent = this.dataSource.getValue();
    recent.push(notification);
    while (recent.length > 10) {
      recent.shift();
    }
    this.dataSource.next(recent);
    localStorage.setItem(this.KEY, JSON.stringify(recent));
  }

  queueNotifications(notifications: CdNotificationConfig[]) {
    this.queuedNotifications = this.queuedNotifications.concat(notifications);
    this.cancel(this.queueTimeoutId);
    this.queueTimeoutId = window.setTimeout(() => {
      this.sendQueuedNotifications();
    }, 500);
  }

  private sendQueuedNotifications() {
    _.uniqWith(this.queuedNotifications, _.isEqual).forEach((notification) => {
      this.show(notification);
    });
    this.queuedNotifications = [];
  }

  /**
   * Method for showing a notification.
   * @param {NotificationType} type toastr type
   * @param {string} title
   * @param {string} [message] The message to be displayed. Note, use this field
   *   for error notifications only.
   * @param {*} [options] toastr compatible options, used when creating a toastr
   * @param {string} [application] Only needed if notification comes from an external application
   * @returns The timeout ID that is set to be able to cancel the notification.
   */
  show(
    type: NotificationType,
    title: string,
    message?: string,
    options?: any | ToastOptions,
    application?: string
  ): number;
  show(config: CdNotificationConfig | (() => CdNotificationConfig)): number;
  show(
    arg: NotificationType | CdNotificationConfig | (() => CdNotificationConfig),
    title?: string,
    message?: string,
    options?: any | ToastOptions,
    application?: string
  ): number {
    return window.setTimeout(() => {
      let config: CdNotificationConfig;
      if (_.isFunction(arg)) {
        config = arg() as CdNotificationConfig;
      } else if (_.isObject(arg)) {
        config = arg as CdNotificationConfig;
      } else {
        config = new CdNotificationConfig(
          arg as NotificationType,
          title,
          message,
          options,
          application
        );
      }
      const notification = new CdNotification(config);
      this.save(notification);
      this.showToasty(notification);
    }, 10);
  }

  private showToasty(notification: CdNotification) {
    // Exit immediately if no toasty should be displayed.
    if (this.hideToasties) {
      return;
    }
    this.toastr[['error', 'info', 'success'][notification.type]](
      (notification.message ? notification.message + '<br>' : '') +
        this.renderTimeAndApplicationHtml(notification),
      notification.title,
      notification.options
    );
  }

  renderTimeAndApplicationHtml(notification: CdNotification): string {
    return `<small class="date">${this.cdDatePipe.transform(
      notification.timestamp
    )}</small><i class="pull-right custom-icon ${notification.applicationClass}" title="${
      notification.application
    }"></i>`;
  }

  notifyTask(finishedTask: FinishedTask, success: boolean = true): number {
    let notification: CdNotificationConfig;
    if (finishedTask.success && success) {
      notification = new CdNotificationConfig(
        NotificationType.success,
        this.taskMessageService.getSuccessTitle(finishedTask)
      );
    } else {
      notification = new CdNotificationConfig(
        NotificationType.error,
        this.taskMessageService.getErrorTitle(finishedTask),
        this.taskMessageService.getErrorMessage(finishedTask)
      );
    }
    return this.show(notification);
  }

  /**
   * Prevent the notification from being shown.
   * @param {number} timeoutId A number representing the ID of the timeout to be canceled.
   */
  cancel(timeoutId) {
    window.clearTimeout(timeoutId);
  }

  /**
   * Suspend showing the notification toasties.
   * @param {boolean} suspend Set to ``true`` to disable/hide toasties.
   */
  suspendToasties(suspend: boolean) {
    this.hideToasties = suspend;
  }
}
