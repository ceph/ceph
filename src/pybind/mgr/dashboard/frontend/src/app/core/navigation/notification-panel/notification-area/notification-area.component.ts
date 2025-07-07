import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs';
import { NotificationService } from '../../../../shared/services/notification.service';
import { CdNotification } from '../../../../shared/models/cd-notification';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';

@Component({
  selector: 'cd-notification-area',
  templateUrl: './notification-area.component.html',
  styleUrls: ['./notification-area.component.scss']
})
export class NotificationAreaComponent implements OnInit, OnDestroy {
  todayNotifications: CdNotification[] = [];
  previousNotifications: CdNotification[] = [];
  private sub: Subscription;

  readonly notificationIconMap = {
    [NotificationType.success]: 'success',
    [NotificationType.error]: 'danger',
    [NotificationType.info]: 'info',
    [NotificationType.warning]: 'warning'
  } as const;

  constructor(private notificationService: NotificationService) {}

  ngOnInit(): void {
    this.sub = this.notificationService.data$.subscribe((notifications: CdNotification[]) => {
      const today: Date = new Date();
      this.todayNotifications = [];
      this.previousNotifications = [];
      notifications.forEach((n: CdNotification) => {
        const notifDate = new Date(n.timestamp);
        if (
          notifDate.getDate() === today.getDate() &&
          notifDate.getMonth() === today.getMonth() &&
          notifDate.getFullYear() === today.getFullYear()
        ) {
          this.todayNotifications.push(n);
        } else {
          this.previousNotifications.push(n);
        }
      });
    });
  }

  ngOnDestroy(): void {
    if (this.sub) {
      this.sub.unsubscribe();
    }
  }

  removeNotification(notification: CdNotification, event: MouseEvent) {
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
    }
  }
}
