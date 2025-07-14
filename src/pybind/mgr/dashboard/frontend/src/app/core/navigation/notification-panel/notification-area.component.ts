import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs';
import { NotificationService } from 'src/app/shared/services/notification.service';
import { CdNotification } from 'src/app/shared/models/cd-notification';
import { NotificationType } from 'src/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-notification-area',
  templateUrl: './notification-area.component.html',
  styleUrls: ['./notification-area.component.scss']
})
export class NotificationAreaComponent implements OnInit, OnDestroy {
  todayNotifications: CdNotification[] = [];
  previousNotifications: CdNotification[] = [];
  private sub: Subscription;

  constructor(private notificationService: NotificationService) {}

  ngOnInit(): void {
    this.sub = this.notificationService.data$.subscribe((notifications) => {
      const today = new Date();
      this.todayNotifications = [];
      this.previousNotifications = [];
      notifications.forEach((n) => {
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
      case NotificationType.success: return 'icon-success';
      case NotificationType.error: return 'icon-error';
      case NotificationType.info: return 'icon-info';
      case NotificationType.warning: return 'icon-warning';
      default: return '';
    }
  }
} 