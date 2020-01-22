import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { TooltipConfig } from 'ngx-bootstrap/tooltip';

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
export class WorkbenchLayoutComponent {
  constructor(private router: Router, public notificationService: NotificationService) {}

  isDashboardPage() {
    return this.router.url === '/dashboard';
  }
}
