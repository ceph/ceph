import { Component, ViewContainerRef } from '@angular/core';
import { AuthStorageService } from './shared/services/auth-storage.service';
import { ToastsManager } from 'ng2-toastr';
import { Router } from '@angular/router';

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

  isLogginActive() {
    return this.router.url === '/login' || !this.authStorageService.isLoggedIn();
  }

}
