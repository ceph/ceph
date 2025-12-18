import { Component, OnInit, OnDestroy, ChangeDetectionStrategy } from '@angular/core';
import { Subscription } from 'rxjs';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CdNotification } from '~/app/shared/models/cd-notification';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { PrometheusNotificationService } from '~/app/shared/services/prometheus-notification.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

@Component({
  selector: 'cd-notifications-page',
  templateUrl: './notifications-page.component.html',
  styleUrls: ['./notifications-page.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: false
})
export class NotificationsPageComponent implements OnInit, OnDestroy {
  notifications: CdNotification[] = [];
  selectedNotificationID: string | null = null;
  searchText: string = '';
  filteredNotifications: CdNotification[] = [];
  private sub: Subscription;
  private interval: number;

  constructor(
    private notificationService: NotificationService,
    private prometheusAlertService: PrometheusAlertService,
    private prometheusNotificationService: PrometheusNotificationService,
    private authStorageService: AuthStorageService
  ) {}

  ngOnInit(): void {
    // Check permissions and trigger Prometheus alerts refresh
    const permissions = this.authStorageService.getPermissions();
    if (permissions.prometheus.read && permissions.configOpt.read) {
      this.triggerPrometheusAlerts();
      // Set up periodic refresh similar to sidebar component
      this.interval = window.setInterval(() => {
        this.triggerPrometheusAlerts();
      }, 5000);
    }

    // Subscribe to notifications from the service
    this.sub = this.notificationService.data$.subscribe((notifications) => {
      this.notifications = notifications;

      // preserve filtered array reference if search active
      if (!this.searchText) {
        this.filteredNotifications = notifications;
      } else {
        this.onSearch(this.searchText);
      }
    });
  }

  ngOnDestroy(): void {
    if (this.sub) {
      this.sub.unsubscribe();
    }
    if (this.interval) {
      window.clearInterval(this.interval);
    }
  }

  onNotificationSelect(notification: CdNotification): void {
    this.selectedNotificationID = notification.id;
  }

  get selectedNotification(): CdNotification {
    return this.filteredNotifications.find((n) => n.id === this.selectedNotificationID);
  }

  onSearch(value: string): void {
    this.searchText = value;
    if (!value || value.trim() === '') {
      this.filteredNotifications = this.notifications;
    } else {
      const searchLower = value.toLowerCase();
      this.filteredNotifications = this.notifications.filter(
        (notification) =>
          notification.title?.toLowerCase().includes(searchLower) ||
          notification.message?.toLowerCase().includes(searchLower) ||
          notification.application?.toLowerCase().includes(searchLower)
      );
    }
  }

  removeNotification(notification: CdNotification, event: MouseEvent): void {
    // Stop event propagation to prevent panel closing
    event.stopPropagation();
    event.preventDefault();

    // Get the notification index from the service's data
    const notifications = this.notificationService['dataSource'].getValue();
    const index = notifications.findIndex((n) => n.id === notification.id);

    if (index > -1) {
      // Remove the notification through the service
      this.notificationService.remove(index);

      // Clear selection if the removed notification was selected
      if (this.selectedNotificationID === notification.id) {
        this.selectedNotificationID = null;
      }
    }
  }

  getCarbonIcon(type: NotificationType | string): string {
    switch (type) {
      case NotificationType.success:
        return 'checkmark--filled';
      case NotificationType.error:
        return 'error--filled';
      case NotificationType.info:
        return 'information--filled';
      case NotificationType.warning:
        return 'warning--filled';
      default:
        return 'notification--filled';
    }
  }

  getIconColorClass(type: NotificationType | string): string {
    switch (type) {
      case NotificationType.success:
        return 'icon-success';
      case NotificationType.error:
        return 'icon-error';
      case NotificationType.info:
        return 'icon-info';
      case NotificationType.warning:
        return 'icon-warning';
      default:
        return '';
    }
  }

  formatDate(timestamp: string): string {
    const date = new Date(timestamp);
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    if (date.toDateString() === today.toDateString()) {
      return 'Today';
    } else if (date.toDateString() === yesterday.toDateString()) {
      return 'Yesterday';
    } else {
      return date.toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric'
      });
    }
  }

  formatTime(timestamp: string): string {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', {
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    });
  }

  private triggerPrometheusAlerts(): void {
    this.prometheusAlertService.refresh();
    this.prometheusNotificationService.refresh();
  }

  trackByNotificationId(_index: number, notification: CdNotification): string {
    return notification.id;
  }
}
