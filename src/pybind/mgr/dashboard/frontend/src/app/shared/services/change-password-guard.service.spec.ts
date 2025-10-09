import { Component, NgZone } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ActivatedRouteSnapshot, Router, RouterStateSnapshot, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { AuthStorageService } from './auth-storage.service';
import { ChangePasswordGuardService } from './change-password-guard.service';

describe('ChangePasswordGuardService', () => {
  let service: ChangePasswordGuardService;
  let authStorageService: AuthStorageService;
  let ngZone: NgZone;
  let route: ActivatedRouteSnapshot;
  let state: RouterStateSnapshot;

  @Component({ selector: 'cd-login-password-form', template: '' })
  class LoginPasswordFormComponent {}

  const routes: Routes = [{ path: 'login-change-password', component: LoginPasswordFormComponent }];

  configureTestBed({
    imports: [RouterTestingModule.withRoutes(routes)],
    providers: [ChangePasswordGuardService, AuthStorageService],
    declarations: [LoginPasswordFormComponent]
  });

  beforeEach(() => {
    service = TestBed.inject(ChangePasswordGuardService);
    authStorageService = TestBed.inject(AuthStorageService);
    ngZone = TestBed.inject(NgZone);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should do nothing (not logged in)', () => {
    spyOn(authStorageService, 'isLoggedIn').and.returnValue(false);
    expect(service.canActivate(route, state)).toBeTruthy();
  });

  it('should do nothing (SSO enabled)', () => {
    spyOn(authStorageService, 'isLoggedIn').and.returnValue(true);
    spyOn(authStorageService, 'isSSO').and.returnValue(true);
    expect(service.canActivate(route, state)).toBeTruthy();
  });

  it('should do nothing (no update pwd required)', () => {
    spyOn(authStorageService, 'isLoggedIn').and.returnValue(true);
    spyOn(authStorageService, 'getPwdUpdateRequired').and.returnValue(false);
    expect(service.canActivate(route, state)).toBeTruthy();
  });

  it('should redirect to change password page by preserving the query params', fakeAsync(() => {
    route = null;
    state = { url: '/host', root: null };
    spyOn(authStorageService, 'isLoggedIn').and.returnValue(true);
    spyOn(authStorageService, 'isSSO').and.returnValue(false);
    spyOn(authStorageService, 'getPwdUpdateRequired').and.returnValue(true);
    const router = TestBed.inject(Router);
    ngZone.run(() => {
      expect(service.canActivate(route, state)).toBeFalsy();
    });
    tick();
    expect(router.url).toBe('/login-change-password?returnUrl=%2Fhost');
  }));
});
