import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbAlertModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { UserFormModel } from '../../../core/auth/user-form/user-form.model';
import { MgrModuleService } from '../../api/mgr-module.service';
import { UserService } from '../../api/user.service';
import { PipesModule } from '../../pipes/pipes.module';
import { AuthStorageService } from '../../services/auth-storage.service';
import { NotificationService } from '../../services/notification.service';
import { TelemetryNotificationService } from '../../services/telemetry-notification.service';
import { TelemetryNotificationComponent } from './telemetry-notification.component';

describe('TelemetryActivationNotificationComponent', () => {
  let component: TelemetryNotificationComponent;
  let fixture: ComponentFixture<TelemetryNotificationComponent>;

  let authStorageService: AuthStorageService;
  let userService: UserService;
  let mgrModuleService: MgrModuleService;
  let notificationService: NotificationService;

  let isNotificationHiddenSpy: jasmine.Spy;
  let getUsernameSpy: jasmine.Spy;
  let userServiceGetSpy: jasmine.Spy;
  let getConfigSpy: jasmine.Spy;

  const user: UserFormModel = {
    username: 'username',
    password: undefined,
    name: 'User 1',
    email: 'user1@email.com',
    roles: ['read-only'],
    enabled: true,
    pwdExpirationDate: undefined,
    pwdUpdateRequired: true
  };
  const admin: UserFormModel = {
    username: 'admin',
    password: undefined,
    name: 'User 1',
    email: 'user1@email.com',
    roles: ['administrator'],
    enabled: true,
    pwdExpirationDate: undefined,
    pwdUpdateRequired: true
  };
  const telemetryEnabledConfig = {
    enabled: true
  };
  const telemetryDisabledConfig = {
    enabled: false
  };

  configureTestBed({
    declarations: [TelemetryNotificationComponent],
    imports: [NgbAlertModule, HttpClientTestingModule, ToastrModule.forRoot(), PipesModule],
    providers: [MgrModuleService, UserService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TelemetryNotificationComponent);
    component = fixture.componentInstance;
    authStorageService = TestBed.inject(AuthStorageService);
    userService = TestBed.inject(UserService);
    mgrModuleService = TestBed.inject(MgrModuleService);
    notificationService = TestBed.inject(NotificationService);

    isNotificationHiddenSpy = spyOn(component, 'isNotificationHidden').and.returnValue(false);
    getUsernameSpy = spyOn(authStorageService, 'getUsername').and.returnValue('username');
    userServiceGetSpy = spyOn(userService, 'get').and.returnValue(of(admin)); // Not the best name but it sounded better than `getSpy`
    getConfigSpy = spyOn(mgrModuleService, 'getConfig').and.returnValue(
      of(telemetryDisabledConfig)
    );
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should not show notification again if the user closed it before', () => {
    isNotificationHiddenSpy.and.returnValue(true);
    fixture.detectChanges();
    expect(component.displayNotification).toBe(false);
  });

  it('should not show notification for an user without administrator role', () => {
    userServiceGetSpy.and.returnValue(of(user));
    fixture.detectChanges();
    expect(component.displayNotification).toBe(false);
  });

  it('should not show notification if the module is enabled already', () => {
    getUsernameSpy.and.returnValue('admin');
    getConfigSpy.and.returnValue(of(telemetryEnabledConfig));
    fixture.detectChanges();
    expect(component.displayNotification).toBe(false);
  });

  it('should show the notification if all pre-conditions set accordingly', () => {
    fixture.detectChanges();
    expect(component.displayNotification).toBe(true);
  });

  it('should hide the notification if the user closes it', () => {
    spyOn(notificationService, 'show');
    fixture.detectChanges();
    component.close();
    expect(notificationService.show).toHaveBeenCalled();
    expect(localStorage.getItem('telemetry_notification_hidden')).toBe('true');
  });

  it('should hide the notification if the user logs out', () => {
    const telemetryNotificationService = TestBed.inject(TelemetryNotificationService);
    spyOn(telemetryNotificationService, 'setVisibility');
    fixture.detectChanges();
    component.ngOnDestroy();
    expect(telemetryNotificationService.setVisibility).toHaveBeenCalledWith(false);
  });
});
