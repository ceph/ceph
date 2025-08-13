import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CdNotification } from '~/app/shared/models/cd-notification';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-notifications-page',
  templateUrl: './notifications-page.component.html',
  styleUrls: ['./notifications-page.component.scss']
})
export class NotificationsPageComponent implements OnInit, OnDestroy {
  notifications: CdNotification[] = [];
  selectedNotification: CdNotification | null = null;
  searchText: string = '';
  filteredNotifications: CdNotification[] = [];
  private sub: Subscription;

  constructor(private notificationService: NotificationService) {
    console.log('NotificationsPageComponent constructor called');
  }

  ngOnInit(): void {
    console.log('NotificationsPageComponent ngOnInit called');

    // Subscribe to notifications from the service
    this.sub = this.notificationService.data$.subscribe((notifications) => {
      this.notifications = notifications;
      this.filteredNotifications = notifications;
      console.log('Notifications loaded:', this.notifications.length);
    });
  }

  ngOnDestroy(): void {
    if (this.sub) {
      this.sub.unsubscribe();
    }
  }

  onNotificationSelect(notification: CdNotification): void {
    this.selectedNotification = notification;
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
    const index = notifications.findIndex(
      (n) => n.timestamp === notification.timestamp && n.title === notification.title
    );

    if (index > -1) {
      // Remove the notification through the service
      this.notificationService.remove(index);

      // Clear selection if the removed notification was selected
      if (this.selectedNotification === notification) {
        this.selectedNotification = null;
      }
    }
  }

  getCarbonIcon(type: NotificationType): string {
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

  getIconColorClass(type: NotificationType): string {
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
}
