import { Component, OnDestroy, OnInit } from '@angular/core';

import { Subscription } from 'rxjs';

import { Icons } from '~/app/shared/enum/icons.enum';
import { CdNotification } from '~/app/shared/models/cd-notification';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';

@Component({
  selector: 'cd-notifications',
  templateUrl: './notifications.component.html',
  styleUrls: ['./notifications.component.scss']
})
export class NotificationsComponent implements OnInit, OnDestroy {
  icons = Icons;
  hasRunningTasks = false;
  hasNotifications = false;
  private subs = new Subscription();

  constructor(
    public notificationService: NotificationService,
    private summaryService: SummaryService
  ) {}

  ngOnInit() {
    this.subs.add(
      this.summaryService.subscribe((summary) => {
        this.hasRunningTasks = summary.executing_tasks.length > 0;
      })
    );

    this.subs.add(
      this.notificationService.data$.subscribe((notifications: CdNotification[]) => {
        this.hasNotifications = notifications.length > 0;
      })
    );
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }
}
