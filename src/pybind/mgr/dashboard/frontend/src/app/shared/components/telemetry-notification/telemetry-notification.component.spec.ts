import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbAlertModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { UserService } from '~/app/shared/api/user.service';
import { AlertPanelComponent } from '~/app/shared/components/alert-panel/alert-panel.component';
import { Permissions } from '~/app/shared/models/permissions';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TelemetryNotificationService } from '~/app/shared/services/telemetry-notification.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { TelemetryNotificationComponent } from './telemetry-notification.component';
import { BlockUIModule } from 'ng-block-ui';

describe('TelemetryActivationNotificationComponent', () => {
  let component: TelemetryNotificationComponent;
  let fixture: ComponentFixture<TelemetryNotificationComponent>;

  let authStorageService: AuthStorageService;
  let mgrModuleService: MgrModuleService;
  let notificationService: NotificationService;

  let isNotificationHiddenSpy: jasmine.Spy;
  let getPermissionsSpy: jasmine.Spy;
  let getConfigSpy: jasmine.Spy;

  const configOptPermissions: Permissions = new Permissions({
    'config-opt': ['read', 'create', 'update', 'delete']
  });
  const noConfigOptPermissions: Permissions = new Permissions({});
  const telemetryEnabledConfig = {
    enabled: true
  };
  const telemetryDisabledConfig = {
    enabled: false
  };

  configureTestBed({
    declarations: [TelemetryNotificationComponent, AlertPanelComponent],
    imports: [
      NgbAlertModule,
      HttpClientTestingModule,
      ToastrModule.forRoot(),
      PipesModule,
      BlockUIModule.forRoot()
    ],
    providers: [MgrModuleService, UserService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TelemetryNotificationComponent);
    component = fixture.componentInstance;
    authStorageService = TestBed.inject(AuthStorageService);
    mgrModuleService = TestBed.inject(MgrModuleService);
    notificationService = TestBed.inject(NotificationService);

    isNotificationHiddenSpy = spyOn(component, 'isNotificationHidden').and.returnValue(false);
    getPermissionsSpy = spyOn(authStorageService, 'getPermissions').and.returnValue(
      configOptPermissions
    );
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

  it('should not show notification for a user without configOpt permissions', () => {
    getPermissionsSpy.and.returnValue(noConfigOptPermissions);
    fixture.detectChanges();
    expect(component.displayNotification).toBe(false);
  });

  it('should not show notification if the module is enabled already', () => {
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
    component.onDismissed();
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
