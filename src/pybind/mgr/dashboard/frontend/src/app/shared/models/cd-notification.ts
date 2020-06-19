import { IndividualConfig } from 'ngx-toastr';

import { Icons } from '../enum/icons.enum';
import { NotificationType } from '../enum/notification-type.enum';

export class CdNotificationConfig {
  applicationClass: string;
  isFinishedTask = false;

  private classes = {
    Ceph: 'ceph-icon',
    Prometheus: 'prometheus-icon'
  };

  constructor(
    public type: NotificationType = NotificationType.info,
    public title?: string,
    public message?: string, // Use this for additional information only
    public options?: any | IndividualConfig,
    public application: string = 'Ceph'
  ) {
    this.applicationClass = this.classes[this.application];
  }
}

export class CdNotification extends CdNotificationConfig {
  timestamp: string;
  textClass: string;
  iconClass: string;
  duration: number;

  private textClasses = ['text-danger', 'text-info', 'text-success'];
  private iconClasses = [Icons.warning, Icons.info, Icons.check];

  constructor(private config: CdNotificationConfig = new CdNotificationConfig()) {
    super(config.type, config.title, config.message, config.options, config.application);
    delete this.config;
    /* string representation of the Date object so it can be directly compared
    with the timestamps parsed from localStorage */
    this.timestamp = new Date().toJSON();
    this.iconClass = this.iconClasses[this.type];
    this.textClass = this.textClasses[this.type];
    this.isFinishedTask = config.isFinishedTask;
  }
}
