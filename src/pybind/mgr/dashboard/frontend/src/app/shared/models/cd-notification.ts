import { Icons } from '../enum/icons.enum';
import { NotificationType } from '../enum/notification-type.enum';
import { ToastContent } from 'carbon-components-angular';

export class CdNotificationConfig {
  applicationClass: string;
  isFinishedTask = false;
  options: ToastContent = {
    lowContrast: true,
    type: undefined,
    title: '',
    subtitle: '',
    caption: ''
  };

  // Prometheus-specific metadata
  prometheusAlert?: {
    alertName: string;
    status: string;
    severity: string;
    instance?: string;
    job?: string;
    description: string;
    sourceUrl?: string;
    fingerprint?: string;
  };

  private classes: { [key: string]: string } = {
    Ceph: 'ceph-icon',
    Prometheus: 'prometheus-icon'
  };

  constructor(
    public type: NotificationType = NotificationType.info,
    public title?: string,
    public message?: string, // Use this for additional information only
    options?: ToastContent,
    public application: string = 'Ceph'
  ) {
    this.applicationClass =
      this.classes[this.application as keyof typeof this.classes] || 'ceph-icon';
    if (options) {
      this.options = { ...this.options, ...options };
    }
  }
}

export class CdNotification extends CdNotificationConfig {
  timestamp: string;
  textClass: string;
  iconClass: string;
  duration: number;
  borderClass: string;
  alertSilenced = false;
  silenceId?: string;

  private textClasses = ['text-danger', 'text-info', 'text-success'];
  private iconClasses = [Icons.warning, Icons.info, Icons.check];
  private borderClasses = ['border-danger', 'border-info', 'border-success'];

  constructor(private config: CdNotificationConfig = new CdNotificationConfig()) {
    super(config.type, config.title, config.message, config.options, config.application);

    // Copy Prometheus metadata if present
    if (config.prometheusAlert) {
      this.prometheusAlert = config.prometheusAlert;
    }

    delete this.config;
    /* string representation of the Date object so it can be directly compared
    with the timestamps parsed from localStorage */
    this.timestamp = new Date().toJSON();
    this.iconClass = this.iconClasses[this.type];
    this.textClass = this.textClasses[this.type];
    this.borderClass = this.borderClasses[this.type];
    this.isFinishedTask = config.isFinishedTask;
  }
}
