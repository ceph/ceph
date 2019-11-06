import { Component, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { Sidebar } from 'ng-sidebar';
import { TooltipConfig } from 'ngx-bootstrap/tooltip';

import { AuthStorageService } from './shared/services/auth-storage.service';
import { NotificationService } from './shared/services/notification.service';

@Component({
  selector: 'cd-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
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
export class AppComponent {
  @ViewChild(Sidebar, { static: true })
  sidebar: Sidebar;

  title = 'cd';

  sidebarOpened = false;
  // There is a bug in ng-sidebar that will show the sidebar closing animation
  // when the page is first loaded. This prevents that.
  sidebarAnimate = false;

  constructor(
    private authStorageService: AuthStorageService,
    private router: Router,
    public notificationService: NotificationService
  ) {
    this.notificationService.sidebarSubject.subscribe((forcedClose) => {
      if (forcedClose) {
        this.sidebar.close();
      } else {
        this.sidebarAnimate = true;
        this.sidebarOpened = !this.sidebarOpened;
      }
    });
  }

  isLoginActive() {
    return this.router.url === '/login' || !this.authStorageService.isLoggedIn();
  }

  isDashboardPage() {
    return this.router.url === '/dashboard';
  }
}
