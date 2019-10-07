import { Component, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationService } from '../../../shared/services/notification.service';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-notifications',
  templateUrl: './notifications.component.html',
  styleUrls: ['./notifications.component.scss']
})
export class NotificationsComponent implements OnInit {
  icons = Icons;

  hasRunningTasks = false;

  constructor(
    public notificationService: NotificationService,
    private summaryService: SummaryService
  ) {}

  ngOnInit() {
    this.summaryService.subscribe((data: any) => {
      if (!data) {
        return;
      }
      this.hasRunningTasks = data.executing_tasks.length > 0;
    });
  }

  toggleSidebar() {
    this.notificationService.toggleSidebar();
  }
}
