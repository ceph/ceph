import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of as observableOf } from 'rxjs';

import { AlertModule } from 'ngx-bootstrap/alert';

import { configureTestBed } from '../../../../testing/unit-test-helper';

import { SettingsService } from '../../api/settings.service';
import { AuthStorageService } from '../../services/auth-storage.service';
import { PwdExpirationNotificationComponent } from './pwd-expiration-notification.component';

describe('PwdExpirationNotificationComponent', () => {
  let component: PwdExpirationNotificationComponent;
  let fixture: ComponentFixture<PwdExpirationNotificationComponent>;
  let settingsService: SettingsService;
  let authStorageService: AuthStorageService;

  @Component({ selector: 'cd-fake', template: '' })
  class FakeComponent {}

  const routes: Routes = [{ path: 'login', component: FakeComponent }];

  configureTestBed({
    declarations: [PwdExpirationNotificationComponent, FakeComponent],
    imports: [
      AlertModule.forRoot(),
      HttpClientTestingModule,
      RouterTestingModule.withRoutes(routes)
    ],
    providers: [SettingsService, AuthStorageService]
  });

  describe('password expiration date has been set', () => {
    beforeEach(() => {
      authStorageService = TestBed.inject(AuthStorageService);
      settingsService = TestBed.inject(SettingsService);
      spyOn(authStorageService, 'getPwdExpirationDate').and.returnValue(1645488000);
      spyOn(settingsService, 'getStandardSettings').and.returnValue(
        observableOf({
          user_pwd_expiration_warning_1: 10,
          user_pwd_expiration_warning_2: 5,
          user_pwd_expiration_span: 90
        })
      );
      fixture = TestBed.createComponent(PwdExpirationNotificationComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();
    });

    it('should create', () => {
      component.ngOnInit();
      expect(component).toBeTruthy();
    });

    it('should set warning levels', () => {
      component.ngOnInit();
      expect(component.pwdExpirationSettings.pwdExpirationWarning1).toBe(10);
      expect(component.pwdExpirationSettings.pwdExpirationWarning2).toBe(5);
    });

    it('should calculate password expiration in days', () => {
      const dateValue = Date;
      spyOn(global, 'Date').and.callFake((date) => {
        if (date) {
          return new dateValue(date);
        } else {
          return new Date('2022-02-18T00:00:00.000Z');
        }
      });
      component.ngOnInit();
      expect(component['expirationDays']).toBe(4);
    });

    it('should set alert type warning correctly', () => {
      const dateValue = Date;
      spyOn(global, 'Date').and.callFake((date) => {
        if (date) {
          return new dateValue(date);
        } else {
          return new Date('2022-02-14T00:00:00.000Z');
        }
      });
      component.ngOnInit();
      expect(component['alertType']).toBe('warning');
    });

    it('should set alert type danger correctly', () => {
      const dateValue = Date;
      spyOn(global, 'Date').and.callFake((date) => {
        if (date) {
          return new dateValue(date);
        } else {
          return new Date('2022-02-18T00:00:00.000Z');
        }
      });
      component.ngOnInit();
      expect(component['alertType']).toBe('danger');
    });
  });

  describe('password expiration date has not been set', () => {
    beforeEach(() => {
      authStorageService = TestBed.inject(AuthStorageService);
      spyOn(authStorageService, 'getPwdExpirationDate').and.returnValue(null);
      fixture = TestBed.createComponent(PwdExpirationNotificationComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();
    });

    it('should calculate no expirationDays', () => {
      component.ngOnInit();
      expect(component['expirationDays']).toBeUndefined();
    });
  });
});
