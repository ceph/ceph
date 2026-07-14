import { Injectable, NgZone } from '@angular/core';

import _ from 'lodash';
import { BehaviorSubject } from 'rxjs';
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
  private readonly MAX_NOTIFICATIONS = 10;
  private readonly SHOW_DELAY = 10;
  private readonly QUEUE_DELAY = 500;
  private readonly ERROR_TOAST_DURATION = 10000;
  private readonly DEFAULT_TOAST_DURATION = 5000;
  private readonly LOCAL_STORAGE_KEY = 'cdNotifications';
  private readonly LOCAL_STORAGE_MUTE_KEY = 'cdNotificationsMuted';
  private readonly LOCAL_STORAGE_READ_KEY = 'cdNotificationsRead';

  private dataSource = new BehaviorSubject<CdNotification[]>([]);
  private panelState = new BehaviorSubject<boolean>(false);
  private muteStateSource = new BehaviorSubject<boolean>(false);
  private activeToastsSource = new BehaviorSubject<ToastContent[]>([]);
  private hasUnreadSource = new BehaviorSubject<boolean>(false);
  private readMapSource = new BehaviorSubject<Record<string, boolean>>({});

  data$ = this.dataSource.asObservable();
  panelState$ = this.panelState.asObservable();
  muteState$ = this.muteStateSource.asObservable();
  activeToasts$ = this.activeToastsSource.asObservable();
  hasUnread$ = this.hasUnreadSource.asObservable();
  readMap$ = this.readMapSource.asObservable();

  private activeToasts: ToastContent[] = [];
  private queued: CdNotificationConfig[] = [];
  private queuedTimeoutId?: number;
  private hideToasties = false;

  constructor(
    private taskMessageService: TaskMessageService,
    private cdDatePipe: CdDatePipe,
    private ngZone: NgZone
  ) {
    this._loadReadMap();
    this._loadStoredNotifications();
    this._loadMutedState();
  }

  private _loadStoredNotifications() {
    const stringNotifications = localStorage.getItem(this.LOCAL_STORAGE_KEY);
    let notifications: CdNotification[] = [];
    if (_.isString(stringNotifications)) {
      try {
        notifications = JSON.parse(stringNotifications, (_key, value) => {
          if (_.isPlainObject(value)) {
            return _.assign(new CdNotification(), value);
          }
          return value;
        });
      } catch {
        localStorage.removeItem(this.LOCAL_STORAGE_KEY);
        notifications = [];
      }
    }
    this.dataSource.next(notifications);
    this._recomputeHasUnread(notifications);
  }

  private _loadReadMap() {
    try {
      this.readMapSource.next(JSON.parse(localStorage.getItem(this.LOCAL_STORAGE_READ_KEY)) || {});
    } catch {
      this.readMapSource.next({});
    }
  }

  private _persistReadMap(readMap: Record<string, boolean>) {
    localStorage.setItem(this.LOCAL_STORAGE_READ_KEY, JSON.stringify(readMap));
  }

  private _recomputeHasUnread(notifications: CdNotification[]) {
    const readMap = this.readMapSource.getValue();
    this.hasUnreadSource.next(notifications.some((n) => !readMap[n.id]));
  }

  private _loadMutedState() {
    const isMuted = localStorage.getItem(this.LOCAL_STORAGE_MUTE_KEY) === 'true';
    this.hideToasties = isMuted;
    this.muteStateSource.next(isMuted);
  }

  private _persistNotifications(notifications: CdNotification[]) {
    try {
      localStorage.setItem(this.LOCAL_STORAGE_KEY, JSON.stringify(notifications));
    } catch (e) {
      const fallback = notifications.slice(0, 10);
      localStorage.removeItem(this.LOCAL_STORAGE_KEY);
      localStorage.setItem(this.LOCAL_STORAGE_KEY, JSON.stringify(fallback));
      this.dataSource.next(fallback);
      this._recomputeHasUnread(fallback);
    }
  }

  // ============
  // STORAGE API
  // ============

  /**
   * Gets all notifications from local storage
   */
  getNotificationsSnapshot(): CdNotification[] {
    return this.dataSource.getValue();
  }

  /**
   * Saving a shown notification in local storage
   */
  save(notification: CdNotification) {
    const current = this.dataSource.getValue();
    const existing = current.find(
      (n) => n.title === notification.title && n.type === notification.type
    );

    let notifications: CdNotification[];
    if (existing) {
      existing.occurrences = (existing.occurrences || 1) + 1;
      existing.timestamp = notification.timestamp;
      notifications = [...current];
    } else {
      notifications = [notification, ...current];
    }

    const limited = notifications
      .sort((a, b) => (a.timestamp > b.timestamp ? -1 : 1))
      .slice(0, this.MAX_NOTIFICATIONS);

    this.dataSource.next(limited);
    this._recomputeHasUnread(limited);
    this._persistNotifications(limited);
  }

  /**
   * Removes a single saved notification from local storage
   */
  remove(index: number) {
    const notifications = [...this.dataSource.getValue()];
    notifications.splice(index, 1);
    this.dataSource.next(notifications);
    this._recomputeHasUnread(notifications);
    this._persistNotifications(notifications);
  }

  removeById(id: string): boolean {
    const notifications = this.dataSource.getValue();
    const index = notifications.findIndex((n) => n.id === id);
    if (index === -1) return false;
    this.remove(index);
    return true;
  }

  /**
   * Removes all current saved notifications from storage (and any appearing toasts)
   */
  removeAll() {
    localStorage.removeItem(this.LOCAL_STORAGE_KEY);
    localStorage.removeItem(this.LOCAL_STORAGE_READ_KEY);
    this.readMapSource.next({});
    this.dataSource.next([]);
    this.hasUnreadSource.next(false);
    this.clearAllToasts();
  }

  /**
   * Method for showing a toast notification
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
      this._queueToShow(config);
    }, this.SHOW_DELAY);
  }

  private _queueToShow(config: CdNotificationConfig) {
    this.cancel(this.queuedTimeoutId);
    if (!this.queued.find((c) => _.isEqual(c, config))) {
      this.queued.push(config);
    }
    this.queuedTimeoutId = window.setTimeout(() => {
      this._showQueued();
    }, this.QUEUE_DELAY);
  }

  private _showQueued() {
    this._getUnifiedTitleQueue().forEach((config) => {
      const notification = new CdNotification(config);

      if (!notification.isFinishedTask) {
        this.save(notification);
      }
      this._showToasty(notification);
    });
    this.queued = [];
  }

  private _getUnifiedTitleQueue(): CdNotificationConfig[] {
    return Object.values(this._queueShiftByTitle()).map((configs) => {
      const config = configs[0];
      if (configs.length > 1) {
        config.message = '<ul>' + configs.map((c) => `<li>${c.message}</li>`).join('') + '</ul>';
      }
      return config;
    });
  }

  private _queueShiftByTitle(): { [key: string]: CdNotificationConfig[] } {
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

  private _showToasty(notification: CdNotification) {
    // Exit immediately if no toasty should be displayed.
    if (this.hideToasties) {
      return;
    }

    // Map notification types to Carbon types
    const carbonType = this.NOTIFICATION_TYPE_MAP[notification.type] || 'info';
    const lowContrast = notification.options?.lowContrast || false;

    const existing = this.activeToasts.find(
      (t) => t.title === notification.title && t.type === carbonType
    );
    if (existing) {
      existing.duplicateCount = (existing.duplicateCount || 1) + 1;
      const count = existing.duplicateCount - 1;
      existing.subtitle = `<span class="toast-message">${existing.originalSubtitle}</span><span class="toast-duplicate-count">(+${count} more)</span>`;
      existing.caption = this._renderTimeAndApplicationHtml(notification);
      this.activeToastsSource.next([...this.activeToasts]);
      return;
    }

    const subtitle = notification.message || '';
    const toast: ToastContent = {
      title: notification.title,
      subtitle,
      caption: this._renderTimeAndApplicationHtml(notification),
      type: carbonType,
      lowContrast: lowContrast,
      showClose: true,
      duration:
        notification.options?.timeOut ||
        (notification.type === NotificationType.error
          ? this.ERROR_TOAST_DURATION
          : this.DEFAULT_TOAST_DURATION),
      notificationId: notification.id,
      duplicateCount: 1,
      originalSubtitle: subtitle
    };

    this.activeToasts.unshift(toast);
    this.activeToastsSource.next(this.activeToasts);

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

  private _renderTimeAndApplicationHtml(notification: CdNotification): string {
    return `<div class="toast-caption-container">
      <small class="date">${this.cdDatePipe.transform(notification.timestamp)}</small>
      <a class="toast-view-more cds--type-label-01" href="#/notifications?id=${notification.id}" i18n>View more</a>
    </div>`;
  }

  clearAllToasts() {
    this.activeToasts = [];
    this.activeToastsSource.next(this.activeToasts);
  }

  /**
   * Suspend showing the notification toasties.
   * @param {boolean} suspend Set to ``true`` to disable/hide toasties.
   */
  suspendToasties(suspend: boolean) {
    this.hideToasties = suspend;
    this.muteStateSource.next(suspend);
    localStorage.setItem(this.LOCAL_STORAGE_MUTE_KEY, suspend.toString());
  }

  removeToast(toast: ToastContent) {
    this.activeToasts = this.activeToasts.filter((t) => !_.isEqual(t, toast));
    this.activeToastsSource.next(this.activeToasts);
  }

  /**
   * Prevent the notification from being shown.
   * @param {number} timeoutId A number representing the ID of the timeout to be canceled.
   */
  cancel(timeoutId?: number) {
    window.clearTimeout(timeoutId);
  }

  // ==================
  // Task Notifications
  // ==================

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

  // =================
  // NOTIFICATION PANEL
  // =================

  /**
   * Toggle the sidebar/panel visibility
   * @param isOpen whether to open or close the panel
   */
  togglePanel(isOpen: boolean) {
    this.panelState.next(isOpen);
  }

  setPanelState(isOpen: boolean) {
    this.panelState.next(isOpen);
  }

  getPanelState(): boolean {
    return this.panelState.value;
  }

  markAsRead(id: string) {
    const current = this.readMapSource.getValue();
    if (current[id]) return;
    const updated = { ...current, [id]: true };
    this.readMapSource.next(updated);
    this._persistReadMap(updated);
    this._recomputeHasUnread(this.dataSource.getValue());
  }

  markAllAsRead() {
    const notifications = this.dataSource.getValue();
    const updated = { ...this.readMapSource.getValue() };
    notifications.forEach((n) => (updated[n.id] = true));
    this.readMapSource.next(updated);
    this._persistReadMap(updated);
    this._recomputeHasUnread(notifications);
  }
}
