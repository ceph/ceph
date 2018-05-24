import { Component } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { AuthGuardService } from './auth-guard.service';
import { AuthStorageService } from './auth-storage.service';

describe('AuthGuardService', () => {
  let service: AuthGuardService;

  @Component({selector: 'cd-login', template: ''})
  class LoginComponent { }

  const routes: Routes = [{ path: 'login', component: LoginComponent }];

  const fakeService = {
    isLoggedIn: () => true
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule.withRoutes(routes)],
      providers: [AuthGuardService, { provide: AuthStorageService, useValue: fakeService }],
      declarations: [LoginComponent]
    });

    service = TestBed.get(AuthGuardService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should allow the user if loggedIn', () => {
    expect(service.canActivate(null, null)).toBe(true);
  });

  it(
    'should prevent user if not loggedIn and redirect to login page',
    fakeAsync(() => {
      const router = TestBed.get(Router);
      const authStorageService = TestBed.get(AuthStorageService);
      spyOn(authStorageService, 'isLoggedIn').and.returnValue(false);

      expect(service.canActivate(null, null)).toBe(false);
      tick();
      expect(router.url).toBe('/login');
    })
  );
});
