import { Component, OnInit } from '@angular/core';
import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import _ from 'lodash';

import { AuthService } from '~/app/shared/api/auth.service';
import { Credentials } from '~/app/shared/models/credentials';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';

@Component({
  selector: 'cd-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss'],
  standalone: false
})
export class LoginComponent implements OnInit {
  loginForm!: UntypedFormGroup;
  isLoginActive = false;
  returnUrl!: string;
  postInstalled = false;
  loginInProgress = false;

  constructor(
    private authService: AuthService,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private route: ActivatedRoute,
    private router: Router,
    private fb: UntypedFormBuilder
  ) {
    this.createForm();
  }

  createForm() {
    this.loginForm = this.fb.group({
      username: ['', [Validators.required]],
      password: ['', [Validators.required]]
    });
  }

  ngOnInit() {
    if (this.authStorageService.isLoggedIn()) {
      this.router.navigate(['']);
      return;
    }

    // Make sure all open modal dialogs are closed.
    this.modalService.dismissAll();

    let token: string | null = null;
    if (window.location.hash.indexOf('access_token=') !== -1) {
      token = window.location.hash.split('access_token=')[1];
      const uri = window.location.toString();
      window.history.replaceState({}, document.title, uri.split('?')[0]);
    }

    this.authService.check(token ?? '').subscribe((login: any) => {
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

  submitted = false;
  login(event?: Event) {
    event?.preventDefault();

    if (this.loginInProgress) {
      return;
    }

    if (this.loginForm.invalid) {
      this.submitted = true;
      this.loginForm.markAllAsTouched();
      return;
    }

    this.loginInProgress = true;

    localStorage.setItem('cluster_api_url', window.location.origin);

    // Map reactive form values back to the Credentials model expected by the API
    const credentials = new Credentials();
    credentials.username = this.loginForm.value.username;
    credentials.password = this.loginForm.value.password;

    this.authService.login(credentials).subscribe(
      () => {
        const urlPath = this.postInstalled ? '/' : '/add-storage';
        let url = _.get(this.route.snapshot.queryParams, 'returnUrl', urlPath);
        if (!this.postInstalled && this.route.snapshot.queryParams['returnUrl'] === '/overview') {
          url = '/add-storage';
        }

        if (url === '/add-storage') {
          this.router.navigateByUrl(
            this.router.createUrlTree([url], { queryParams: { welcome: true } })
          );
        } else {
          this.router.navigateByUrl(url);
        }

        this.loginInProgress = false;
      },
      () => {
        this.loginInProgress = false;
      }
    );
  }
}
