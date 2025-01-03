import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import _ from 'lodash';

import { AuthService } from '~/app/shared/api/auth.service';
import { Credentials } from '~/app/shared/models/credentials';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';

@Component({
  selector: 'cd-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent implements OnInit {
  model = new Credentials();
  isLoginActive = false;
  returnUrl: string;
  postInstalled = false;

  constructor(
    private authService: AuthService,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private route: ActivatedRoute,
    private router: Router
  ) {}

  ngOnInit() {
    if (this.authStorageService.isLoggedIn()) {
      this.router.navigate(['']);
    } else {
      // Make sure all open modal dialogs are closed. This might be
      // necessary when the logged in user is redirected to the login
      // page after a 401.
      this.modalService.dismissAll();

      let token: string = null;
      if (window.location.hash.indexOf('access_token=') !== -1) {
        token = window.location.hash.split('access_token=')[1];
        const uri = window.location.toString();
        window.history.replaceState({}, document.title, uri.split('?')[0]);
      }
      this.authService.check(token).subscribe((login: any) => {
        if (login.login_url) {
          this.postInstalled = login.cluster_status === 'POST_INSTALLED';
          if (login.login_url === '#/login') {
            this.isLoginActive = true;
          } else {
            window.location.replace(login.login_url);
          }
        } else {
          this.authStorageService.set(
            login.username,
            login.permissions,
            login.sso,
            login.pwdExpirationDate
          );
          this.router.navigate(['']);
        }
      });
    }
  }

  login() {
    localStorage.setItem('cluster_api_url', window.location.origin);
    this.authService.login(this.model).subscribe(() => {
      const urlPath = this.postInstalled ? '/' : '/expand-cluster';
      let url = _.get(this.route.snapshot.queryParams, 'returnUrl', urlPath);
      if (!this.postInstalled && this.route.snapshot.queryParams['returnUrl'] === '/dashboard') {
        url = '/expand-cluster';
      }
      if (url == '/expand-cluster') {
        this.router.navigate([url], { queryParams: { welcome: true } });
      } else {
        this.router.navigate([url]);
      }
    });
  }
}
