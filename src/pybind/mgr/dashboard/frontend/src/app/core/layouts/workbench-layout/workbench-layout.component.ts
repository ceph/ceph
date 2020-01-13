import { Component, OnDestroy, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { Sidebar } from 'ng-sidebar';
import { TooltipConfig } from 'ngx-bootstrap/tooltip';
import { Subscription } from 'rxjs';

import { NotificationService } from '../../../shared/services/notification.service';

@Component({
  selector: 'cd-workbench-layout',
  templateUrl: './workbench-layout.component.html',
  styleUrls: ['./workbench-layout.component.scss'],
  providers: [
    {
      provide: TooltipConfig,
      useFactory: (): TooltipConfig =>
        Object.assign(new TooltipConfig(), {
          container: 'body'
        })
    }
  ]
})
export class WorkbenchLayoutComponent implements OnDestroy {
  @ViewChild(Sidebar, { static: true })
  sidebar: Sidebar;

  sidebarOpened = false;
  // There is a bug in ng-sidebar that will show the sidebar closing animation
  // when the page is first loaded. This prevents that.
  sidebarAnimate = false;

  private readonly sidebarSubscription: Subscription;

  constructor(private router: Router, public notificationService: NotificationService) {
    this.sidebarSubscription = this.notificationService.sidebarSubject.subscribe((forcedClose) => {
      if (forcedClose) {
        this.sidebar.close();
      } else {
        this.sidebarAnimate = true;
        this.sidebarOpened = !this.sidebarOpened;
      }
    });
  }

  ngOnDestroy() {
    if (this.sidebarSubscription) {
      this.sidebarSubscription.unsubscribe();
    }
  }

  isDashboardPage() {
    return this.router.url === '/dashboard';
  }
}
