import { Component, ViewContainerRef } from '@angular/core';
import { Router } from '@angular/router';

import { ToastsManager } from 'ng2-toastr';

import { AuthStorageService } from './shared/services/auth-storage.service';

@Component({
  selector: 'cd-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  title = 'cd';

  constructor(private authStorageService: AuthStorageService,
              private router: Router,
              public toastr: ToastsManager,
              private vcr: ViewContainerRef) {
    this.toastr.setRootViewContainerRef(vcr);
  }

  isLoginActive() {
    return this.router.url === '/login' || !this.authStorageService.isLoggedIn();
  }

}
