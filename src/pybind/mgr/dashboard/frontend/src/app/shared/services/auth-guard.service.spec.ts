import { Component, NgZone } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { AuthGuardService } from './auth-guard.service';
import { AuthStorageService } from './auth-storage.service';

describe('AuthGuardService', () => {
  let service: AuthGuardService;
  let authStorageService: AuthStorageService;
  let ngZone: NgZone;

  @Component({ selector: 'cd-login', template: '' })
  class LoginComponent {}

  const routes: Routes = [{ path: 'login', component: LoginComponent }];

  configureTestBed({
    imports: [RouterTestingModule.withRoutes(routes)],
    providers: [AuthGuardService, AuthStorageService],
    declarations: [LoginComponent]
  });

  beforeEach(() => {
    service = TestBed.inject(AuthGuardService);
    authStorageService = TestBed.inject(AuthStorageService);
    ngZone = TestBed.inject(NgZone);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should allow the user if loggedIn', () => {
    spyOn(authStorageService, 'isLoggedIn').and.returnValue(true);
    expect(service.canActivate()).toBe(true);
  });

  it('should prevent user if not loggedIn and redirect to login page', fakeAsync(() => {
    const router = TestBed.inject(Router);
    ngZone.run(() => {
      expect(service.canActivate()).toBe(false);
    });
    tick();
    expect(router.url).toBe('/login');
  }));
});
