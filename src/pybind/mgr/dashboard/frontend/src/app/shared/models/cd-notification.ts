import { ToastOptions } from 'ng2-toastr';
import { NotificationType } from '../enum/notification-type.enum';

export class CdNotificationConfig {
  constructor(
    public type: NotificationType,
    public title: string,
    public message?: string, // Use this for error notifications only
    public options?: any | ToastOptions
  ) {}
}

export class CdNotification {
  timestamp: string;

  constructor(
    public type: NotificationType = NotificationType.info,
    public title?: string,
    public message?: string
  ) {
    /* string representation of the Date object so it can be directly compared
    with the timestamps parsed from localStorage */
    this.timestamp = new Date().toJSON();
  }

  textClass() {
    switch (this.type) {
      case NotificationType.error:
        return 'text-danger';
      case NotificationType.info:
        return 'text-info';
      case NotificationType.success:
        return 'text-success';
    }
  }

  iconClass() {
    switch (this.type) {
      case NotificationType.error:
        return 'fa-exclamation-triangle';
      case NotificationType.info:
        return 'fa-info';
      case NotificationType.success:
        return 'fa-check';
    }
  }
}
