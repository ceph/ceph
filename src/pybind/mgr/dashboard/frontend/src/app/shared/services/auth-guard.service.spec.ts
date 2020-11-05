import { Component, NgZone } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ActivatedRouteSnapshot, Router, RouterStateSnapshot, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { AuthGuardService } from './auth-guard.service';
import { AuthStorageService } from './auth-storage.service';

describe('AuthGuardService', () => {
  let service: AuthGuardService;
  let authStorageService: AuthStorageService;
  let ngZone: NgZone;
  let route: ActivatedRouteSnapshot;
  let state: RouterStateSnapshot;

  @Component({ selector: 'cd-login', template: '' })
  class LoginComponent {}

  const routes: Routes = [{ path: 'login', component: LoginComponent }];

  configureTestBed({
    imports: [RouterTestingModule.withRoutes(routes)],
    providers: [AuthGuardService, AuthStorageService],
    declarations: [LoginComponent]
  });

  beforeEach(() => {
    service = TestBed.get(AuthGuardService);
    authStorageService = TestBed.get(AuthStorageService);
    ngZone = TestBed.get(NgZone);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should allow the user if loggedIn', () => {
    route = null;
    state = { url: '/', root: null };
    spyOn(authStorageService, 'isLoggedIn').and.returnValue(true);
    expect(service.canActivate(route, state)).toBe(true);
  });

  it('should prevent user if not loggedIn and redirect to login page', fakeAsync(() => {
    const router = TestBed.get(Router);
    state = { url: '/pool', root: null };
    ngZone.run(() => {
      expect(service.canActivate(route, state)).toBe(false);
    });
    tick();
    expect(router.url).toBe('/login?returnUrl=%2Fpool');
  }));
});
