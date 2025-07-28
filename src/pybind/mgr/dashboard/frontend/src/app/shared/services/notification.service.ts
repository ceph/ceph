import { Injectable } from '@angular/core';

import _ from 'lodash';
import { IndividualConfig, ToastrService } from 'ngx-toastr';
import { BehaviorSubject } from 'rxjs';

import { NotificationType } from '../enum/notification-type.enum';
import { CdNotification, CdNotificationConfig } from '../models/cd-notification';
import { FinishedTask } from '../models/finished-task';
import { CdDatePipe } from '../pipes/cd-date.pipe';
import { TaskMessageService } from './task-message.service';

@Injectable({
  providedIn: 'root'
})
export class NotificationService {
  private hideToasties = false;

  // Data observable
  private dataSource = new BehaviorSubject<CdNotification[]>([]);
  data$ = this.dataSource.asObservable();

  // Panel state observable
  private panelStateSource = new BehaviorSubject<{ isOpen: boolean; useNewPanel: boolean }>({
    isOpen: false,
    useNewPanel: true
  });
  panelState$ = this.panelStateSource.asObservable();

  // Mute state observable
  private muteStateSource = new BehaviorSubject<boolean>(false);
  muteState$ = this.muteStateSource.asObservable();

  private queued: CdNotificationConfig[] = [];
  private queuedTimeoutId: number;
  KEY = 'cdNotifications';
  MUTE_KEY = 'cdNotificationsMuted';

  constructor(
    public toastr: ToastrService,
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

    // Load mute state from localStorage
    const isMuted = localStorage.getItem(this.MUTE_KEY) === 'true';
    this.hideToasties = isMuted;
    this.muteStateSource.next(isMuted);
  }

  /**
   * Removes all current saved notifications
   */
  removeAll() {
    localStorage.removeItem(this.KEY);
    this.dataSource.next([]);
  }

  /**
   * Removes a single saved notification
   */
  remove(index: number) {
    const notifications = this.dataSource.getValue();
    notifications.splice(index, 1);
    this.dataSource.next(notifications);
    this.persistNotifications(notifications);
  }

  /**
   * Method used for saving a shown notification (check show() method).
   */
  save(notification: CdNotification) {
    const notifications = this.dataSource.getValue();
    notifications.push(notification);
    notifications.sort((a, b) => (a.timestamp > b.timestamp ? -1 : 1));
    while (notifications.length > 10) {
      notifications.pop();
    }
    this.dataSource.next(notifications);
    this.persistNotifications(notifications);
  }

  /**
   * Persists notifications to localStorage
   */
  private persistNotifications(notifications: CdNotification[]) {
    localStorage.setItem(this.KEY, JSON.stringify(notifications));
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
    options?: any | IndividualConfig,
    application?: string
  ): number;
  show(config: CdNotificationConfig | (() => CdNotificationConfig)): number;
  show(
    arg: NotificationType | CdNotificationConfig | (() => CdNotificationConfig),
    title?: string,
    message?: string,
    options?: any | IndividualConfig,
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
      this.queueToShow(config);
    }, 10);
  }

  private queueToShow(config: CdNotificationConfig) {
    this.cancel(this.queuedTimeoutId);
    if (!this.queued.find((c) => _.isEqual(c, config))) {
      this.queued.push(config);
    }
    this.queuedTimeoutId = window.setTimeout(() => {
      this.showQueued();
    }, 500);
  }

  private showQueued() {
    this.getUnifiedTitleQueue().forEach((config) => {
      const notification = new CdNotification(config);

      if (!notification.isFinishedTask) {
        this.save(notification);
      }
      this.showToasty(notification);
    });
    this.queued = [];
  }

  private getUnifiedTitleQueue(): CdNotificationConfig[] {
    return Object.values(this.queueShiftByTitle()).map((configs) => {
      const config = configs[0];
      if (configs.length > 1) {
        config.message = '<ul>' + configs.map((c) => `<li>${c.message}</li>`).join('') + '</ul>';
      }
      return config;
    });
  }

  private queueShiftByTitle(): { [key: string]: CdNotificationConfig[] } {
    const byTitle: { [key: string]: CdNotificationConfig[] } = {};
    let config: CdNotificationConfig;
    while ((config = this.queued.shift())) {
      if (!byTitle[config.title]) {
        byTitle[config.title] = [];
      }
      byTitle[config.title].push(config);
    }
    return byTitle;
  }

  private showToasty(notification: CdNotification) {
    // Exit immediately if no toasty should be displayed.
    if (this.hideToasties) {
      return;
    }
    const toastrFn =
      notification.type === NotificationType.error
        ? this.toastr.error.bind(this.toastr)
        : notification.type === NotificationType.info
        ? this.toastr.info.bind(this.toastr)
        : this.toastr.success.bind(this.toastr);

    toastrFn(
      (notification.message ? notification.message + '<br>' : '') +
        this.renderTimeAndApplicationHtml(notification),
      notification.title,
      notification.options
    );
  }

  renderTimeAndApplicationHtml(notification: CdNotification): string {
    return `<small class="date">${this.cdDatePipe.transform(
      notification.timestamp
    )}</small><i class="float-end custom-icon ${notification.applicationClass}" title="${
      notification.application
    }"></i>`;
  }

  notifyTask(finishedTask: FinishedTask, success: boolean = true): number {
    const notification = this.finishedTaskToNotification(finishedTask, success);
    notification.isFinishedTask = true;
    return this.show(notification);
  }

  finishedTaskToNotification(
    finishedTask: FinishedTask,
    success: boolean = true
  ): CdNotificationConfig {
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
    notification.isFinishedTask = true;

    return notification;
  }

  /**
   * Prevent the notification from being shown.
   * @param {number} timeoutId A number representing the ID of the timeout to be canceled.
   */
  cancel(timeoutId: number) {
    window.clearTimeout(timeoutId);
  }

  /**
   * Suspend showing the notification toasties.
   * @param {boolean} suspend Set to ``true`` to disable/hide toasties.
   */
  suspendToasties(suspend: boolean) {
    this.hideToasties = suspend;
    this.muteStateSource.next(suspend);
    localStorage.setItem(this.MUTE_KEY, suspend.toString());
  }

  /**
   * Toggle the sidebar/panel visibility
   * @param isOpen whether to open or close the panel
   * @param useNewPanel which panel type to use
   */
  toggleSidebar(isOpen: boolean, useNewPanel: boolean = true) {
    this.panelStateSource.next({
      isOpen: isOpen,
      useNewPanel: useNewPanel
    });
  }
}
