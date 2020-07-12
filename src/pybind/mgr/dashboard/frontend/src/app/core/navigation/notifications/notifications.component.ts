import { Component, OnDestroy, OnInit } from '@angular/core';

import { Subscription } from 'rxjs';

import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationService } from '../../../shared/services/notification.service';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-notifications',
  templateUrl: './notifications.component.html',
  styleUrls: ['./notifications.component.scss']
})
export class NotificationsComponent implements OnInit, OnDestroy {
  icons = Icons;
  hasRunningTasks = false;
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
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  toggleSidebar() {
    this.notificationService.toggleSidebar();
  }
}
