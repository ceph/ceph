import { NotificationType } from '../enum/notification-type.enum';

export class CdNotification {
  message: string;
  timestamp: string;
  title: string;
  type: NotificationType;

  constructor(type: NotificationType = NotificationType.info, title?: string, message?: string) {
    this.type = type;
    this.title = title;
    this.message = message;

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
