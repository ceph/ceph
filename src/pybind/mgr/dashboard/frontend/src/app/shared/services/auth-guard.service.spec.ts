import { Component } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { AuthGuardService } from './auth-guard.service';
import { AuthStorageService } from './auth-storage.service';

describe('AuthGuardService', () => {
  let service: AuthGuardService;
  let authStorageService: AuthStorageService;

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
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should allow the user if loggedIn', () => {
    spyOn(authStorageService, 'isLoggedIn').and.returnValue(true);
    expect(service.canActivate(null, null)).toBe(true);
  });

  it(
    'should prevent user if not loggedIn and redirect to login page',
    fakeAsync(() => {
      const router = TestBed.get(Router);
      expect(service.canActivate(null, null)).toBe(false);
      tick();
      expect(router.url).toBe('/login');
    })
  );
});
