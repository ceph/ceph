import { ToastOptions } from 'ng2-toastr';
import { NotificationType } from '../enum/notification-type.enum';

export class CdNotificationConfig {
  applicationClass: string;

  private classes = {
    Ceph: 'ceph-icon',
    Prometheus: 'prometheus-icon'
  };

  constructor(
    public type: NotificationType = NotificationType.info,
    public title?: string,
    public message?: string, // Use this for additional information only
    public options?: any | ToastOptions,
    public application: string = 'Ceph'
  ) {
    this.applicationClass = this.classes[this.application];
  }
}

export class CdNotification extends CdNotificationConfig {
  timestamp: string;
  textClass: string;
  iconClass: string;

  private textClasses = ['text-danger', 'text-info', 'text-success'];
  private iconClasses = ['fa-exclamation-triangle', 'fa-info', 'fa-check'];

  constructor(private config: CdNotificationConfig = new CdNotificationConfig()) {
    super(config.type, config.title, config.message, config.options, config.application);
    delete this.config;
    /* string representation of the Date object so it can be directly compared
    with the timestamps parsed from localStorage */
    this.timestamp = new Date().toJSON();
    this.iconClass = this.iconClasses[this.type];
    this.textClass = this.textClasses[this.type];
  }
}
