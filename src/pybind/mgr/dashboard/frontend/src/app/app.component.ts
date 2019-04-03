import { Component, ViewContainerRef } from '@angular/core';
import { Router } from '@angular/router';

import { ToastsManager } from 'ng2-toastr';
import { TooltipConfig } from 'ngx-bootstrap/tooltip';

import { AuthStorageService } from './shared/services/auth-storage.service';

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
  title = 'cd';

  constructor(
    private authStorageService: AuthStorageService,
    private router: Router,
    public toastr: ToastsManager,
    private vcr: ViewContainerRef
  ) {
    this.toastr.setRootViewContainerRef(this.vcr);
  }

  isLoginActive() {
    return this.router.url === '/login' || !this.authStorageService.isLoggedIn();
  }

  isDashboardPage() {
    return this.router.url === '/dashboard';
  }
}
