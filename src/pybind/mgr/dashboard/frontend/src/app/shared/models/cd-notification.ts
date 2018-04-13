import { NotificationType } from '../enum/notification-type.enum';

export class CdNotification {
  message: string;
  timestamp: string;
  title: string;
  type: NotificationType;

  constructor(type: NotificationType = NotificationType.info, message?: string, title?: string) {
    this.type = type;
    this.message = message;
    this.title = title;

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
