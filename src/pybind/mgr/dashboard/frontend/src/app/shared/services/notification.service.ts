import { Injectable } from '@angular/core';

import _ from 'lodash';
import { IndividualConfig, ToastrService } from 'ngx-toastr';
import { BehaviorSubject, Subject } from 'rxjs';

import { TOAST_CONFIG } from '../../app.module';
import { NotificationType } from '../enum/notification-type.enum';
import { CdNotification, CdNotificationConfig } from '../models/cd-notification';
import { FinishedTask } from '../models/finished-task';
import { TaskMessageService } from './task-message.service';

@Injectable({
  providedIn: 'root'
})
export class NotificationService {
  private hideToasties = false;

  // Data observable
  private dataSource = new BehaviorSubject<CdNotification[]>([]);
  data$ = this.dataSource.asObservable();

  // Sidebar observable
  sidebarSubject = new Subject();

  private queued: CdNotificationConfig[] = [];
  private queuedTimeoutId: number;
  KEY = 'cdNotifications';

  constructor(
    public toastr: ToastrService,
    private taskMessageService: TaskMessageService
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
   * Removes a single saved notifications
   */
  remove(index: number) {
    const recent = this.dataSource.getValue();
    recent.splice(index, 1);
    this.dataSource.next(recent);
    localStorage.setItem(this.KEY, JSON.stringify(recent));
  }

  /**
   * Method used for saving a shown notification (check show() method).
   */
  save(notification: CdNotification) {
    const recent = this.dataSource.getValue();
    recent.push(notification);
    recent.sort((a, b) => (a.timestamp > b.timestamp ? -1 : 1));
    while (recent.length > 10) {
      recent.pop();
    }
    this.dataSource.next(recent);
    localStorage.setItem(this.KEY, JSON.stringify(recent));
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
    }, 100);
  }

  private showQueued() {
    this.getUnifiedTitleQueue().forEach((config) => {
      const notification = new CdNotification(config);

      if (!notification.isFinishedTask) {
        this.save(notification);
      }
      
      window.setTimeout(() => {
        this.showToasty(notification);
      }, 50);
    });
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
    
    // Get the notification type as a string for class assignment
    const typeStr = ['error', 'info', 'success'][notification.type];
    
    // Calculate smart timeout based on content length
    const baseTimeout = 2000; // Minimum time (milliseconds)
    const contentLength = 
      (notification.title ? notification.title.length : 0) + 
      (notification.message ? notification.message.length : 0);
    
    // Reading speed: approx 15 chars per second (adjust as needed)
    // Min 2 seconds, max 8 seconds
    const readingTime = Math.min(Math.max(Math.floor(contentLength / 15) * 1000, baseTimeout), 8000);
    
    // Shorter timeouts for success notifications, longer for errors
    const finalTimeout = notification.type === NotificationType.success 
      ? Math.min(readingTime, 4000)  // Success: shorter display time (max 4s)
      : notification.type === NotificationType.error 
        ? Math.max(readingTime, 5000)  // Error: longer display time (min 5s)
        : readingTime;  // Info: standard reading time
    
    // Use branding configuration from app.module
    const useTextBranding = TOAST_CONFIG.brandingEnabled && TOAST_CONFIG.textBranding;
    const brandingClass = !TOAST_CONFIG.brandingEnabled 
      ? '' 
      : (useTextBranding ? ' text-branded' : '');
    
    // Configure custom toast options
    const options = {
      ...notification.options,
      closeButton: true,
      progressBar: true,
      timeOut: finalTimeout,
      positionClass: 'toast-top-right',
      enableHtml: true,
      toastClass: `ngx-toastr toast-${typeStr}${brandingClass}`
    };
    
    // Customize the message content with icon
    const customIcon = this.getCustomIcon(notification.type);
    const customHtml = `
      <div class="custom-toast">
        ${customIcon}
        <div class="toast-content">
          ${notification.message ? notification.message + '<br>' : ''}
          ${this.renderTimeAndApplicationHtml(notification)}
        </div>
      </div>
    `;
    
    // Display the toast with our custom content and options
    if (typeStr === 'error') {
      this.toastr.error(customHtml, notification.title, options);
    } else if (typeStr === 'info') {
      this.toastr.info(customHtml, notification.title, options);
    } else if (typeStr === 'success') {
      this.toastr.success(customHtml, notification.title, options);
    }
  }

  // Helper method to get custom icon based on notification type
  private getCustomIcon(type: NotificationType): string {
    const iconMap = {
      [NotificationType.error]: '<i class="custom-icon fa fa-exclamation-circle"></i>',
      [NotificationType.info]: '<i class="custom-icon fa fa-info-circle"></i>',
      [NotificationType.success]: '<i class="custom-icon fa fa-check-circle"></i>'
    };
    return iconMap[type] || '';
  }

  renderTimeAndApplicationHtml(notification: CdNotification): string {
    const formattedDate = this.formatTimestamp(notification.timestamp);
    return `<div class="toast-metadata">
      <small class="date">${formattedDate}</small>
      <i class="custom-icon ${notification.applicationClass}" title="${notification.application}"></i>
    </div>`;
  }

  private formatTimestamp(timestamp: string): string {
    // Format: "DD/MM/YY HH:MM AM/PM"
    const date = new Date(timestamp);
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const year = date.getFullYear().toString().substring(2);
    
    const hours = date.getHours();
    const displayHours = (hours % 12) || 12;
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const ampm = hours >= 12 ? 'PM' : 'AM';
    
    return `${day}/${month}/${year} ${displayHours}:${minutes} ${ampm}`;
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
  }

  toggleSidebar(forceClose = false) {
    this.sidebarSubject.next(forceClose);
  }
}
