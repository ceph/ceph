import { Component, OnInit } from '@angular/core';

import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdNotification } from '../../../shared/models/cd-notification';
import { NotificationService } from '../../../shared/services/notification.service';

@Component({
  selector: 'cd-notifications',
  templateUrl: './notifications.component.html',
  styleUrls: ['./notifications.component.scss']
})
export class NotificationsComponent implements OnInit {
  notifications: CdNotification[];
  notificationType = NotificationType;

  constructor(private notificationService: NotificationService) {
    this.notifications = [];
  }

  ngOnInit() {
    this.notificationService.data$.subscribe((notifications) => {
      this.notifications = notifications;
    });
  }

  removeAll () {
    this.notificationService.removeAll();
  }
}
