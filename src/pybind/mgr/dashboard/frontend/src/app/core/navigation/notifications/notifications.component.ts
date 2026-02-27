import { Component, OnDestroy, OnInit } from '@angular/core';

import { Subscription } from 'rxjs';

import { ICON_TYPE, IconSize } from '~/app/shared/enum/icons.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';

@Component({
  selector: 'cd-notifications',
  templateUrl: './notifications.component.html',
  styleUrls: ['./notifications.component.scss'],
  standalone: false
})
export class NotificationsComponent implements OnInit, OnDestroy {
  icons = ICON_TYPE;
  iconSize = IconSize.size20;
  hasRunningTasks = false;
  hasNotifications = false;
  isMuted = false;
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
      this.notificationService.muteState$.subscribe((isMuted) => {
        this.isMuted = isMuted;
      })
    );

    this.subs.add(
      this.notificationService.hasUnread$.subscribe(
        (hasUnread) => (this.hasNotifications = hasUnread)
      )
    );
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }
}
