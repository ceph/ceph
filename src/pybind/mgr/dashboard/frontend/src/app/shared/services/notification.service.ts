import { Injectable, NgZone } from '@angular/core';

import _ from 'lodash';
import { BehaviorSubject, Subject } from 'rxjs';
import {
  ToastContent,
  NotificationType as CarbonNotificationType
} from 'carbon-components-angular';

import { NotificationType } from '../enum/notification-type.enum';
import { CdNotification, CdNotificationConfig } from '../models/cd-notification';
import { FinishedTask } from '../models/finished-task';
import { CdDatePipe } from '../pipes/cd-date.pipe';
import { TaskMessageService } from './task-message.service';

@Injectable({
  providedIn: 'root'
})
export class NotificationService {
  private readonly NOTIFICATION_TYPE_MAP: Record<NotificationType, CarbonNotificationType> = {
    [NotificationType.error]: 'error',
    [NotificationType.info]: 'info',
    [NotificationType.success]: 'success',
    [NotificationType.warning]: 'warning'
  };

  private hideToasties = false;

  private dataSource = new BehaviorSubject<CdNotification[]>([]);
  private panelStateSource = new BehaviorSubject<{ isOpen: boolean; useNewPanel: boolean }>({
    isOpen: false,
    useNewPanel: true
  });
  private muteStateSource = new BehaviorSubject<boolean>(false);
  private activeToastsSource = new BehaviorSubject<ToastContent[]>([]);
  sidebarSubject = new Subject();

  data$ = this.dataSource.asObservable();
  panelState$ = this.panelStateSource.asObservable();
  muteState$ = this.muteStateSource.asObservable();
  activeToasts$ = this.activeToastsSource.asObservable();

  private queued: CdNotificationConfig[] = [];
  private queuedTimeoutId: number;
  private activeToasts: ToastContent[] = [];
  KEY = 'cdNotifications';
  MUTE_KEY = 'cdNotificationsMuted';
  private readonly MAX_NOTIFICATIONS = 10;

  constructor(
    private taskMessageService: TaskMessageService,
    private cdDatePipe: CdDatePipe,
    private ngZone: NgZone
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
    while (notifications.length > this.MAX_NOTIFICATIONS) {
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
    options?: ToastContent,
    application?: string
  ): number;
  show(config: CdNotificationConfig | (() => CdNotificationConfig)): number;
  show(
    arg: NotificationType | CdNotificationConfig | (() => CdNotificationConfig),
    title?: string,
    message?: string,
    options?: ToastContent,
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

    // Map notification types to Carbon types
    const carbonType = this.NOTIFICATION_TYPE_MAP[notification.type] || 'info';
    const lowContrast = notification.options?.lowContrast || false;

    const toast: ToastContent = {
      title: notification.title,
      subtitle: notification.message || '',
      caption: this.renderTimeAndApplicationHtml(notification),
      type: carbonType,
      lowContrast: lowContrast,
      showClose: true,
      duration: notification.options?.timeOut || 5000
    };

    // Add new toast to the beginning of the array
    this.activeToasts.unshift(toast);
    this.activeToastsSource.next(this.activeToasts);

    // Handle duration-based auto-dismissal
    if (toast.duration && toast.duration > 0) {
      this.ngZone.runOutsideAngular(() => {
        setTimeout(() => {
          this.ngZone.run(() => {
            this.removeToast(toast);
          });
        }, toast.duration);
      });
    }
  }

  /**
   * Remove a toast
   */
  removeToast(toast: ToastContent) {
    this.activeToasts = this.activeToasts.filter((t) => !_.isEqual(t, toast));
    this.activeToastsSource.next(this.activeToasts);
  }

  renderTimeAndApplicationHtml(notification: CdNotification): string {
    let html = `<div class="toast-caption-container">
      <small class="date">${this.cdDatePipe.transform(notification.timestamp)}</small>`;

    html += '</div>';
    return html;
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

  clearAllToasts() {
    this.activeToasts = [];
    this.activeToastsSource.next(this.activeToasts);
  }
}
