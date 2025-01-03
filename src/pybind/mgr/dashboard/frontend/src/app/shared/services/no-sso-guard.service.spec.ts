import { Component, NgZone } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { DashboardUserDeniedError } from '~/app/core/error/error';
import { configureTestBed } from '~/testing/unit-test-helper';
import { AuthStorageService } from './auth-storage.service';
import { NoSsoGuardService } from './no-sso-guard.service';

describe('NoSsoGuardService', () => {
  let service: NoSsoGuardService;
  let authStorageService: AuthStorageService;
  let ngZone: NgZone;

  @Component({ selector: 'cd-404', template: '' })
  class NotFoundComponent {}

  const routes: Routes = [{ path: '404', component: NotFoundComponent }];

  configureTestBed({
    imports: [RouterTestingModule.withRoutes(routes)],
    providers: [NoSsoGuardService, AuthStorageService],
    declarations: [NotFoundComponent]
  });

  beforeEach(() => {
    service = TestBed.inject(NoSsoGuardService);
    authStorageService = TestBed.inject(AuthStorageService);
    ngZone = TestBed.inject(NgZone);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should allow if not logged in via SSO', () => {
    spyOn(authStorageService, 'isSSO').and.returnValue(false);
    expect(service.canActivate()).toBe(true);
  });

  it('should prevent if logged in via SSO', fakeAsync(() => {
    spyOn(authStorageService, 'isSSO').and.returnValue(true);
    ngZone.run(() => {
      expect(() => service.canActivate()).toThrowError(DashboardUserDeniedError);
    });
    tick();
  }));
});
