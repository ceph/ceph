import { Component, OnInit } from '@angular/core';

import * as _ from 'lodash';

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
    this.notificationService.data$.subscribe((notifications: CdNotification[]) => {
      this.notifications = _.orderBy(notifications, ['timestamp'], ['desc']);
    });
  }

  removeAll () {
    this.notificationService.removeAll();
  }
}
