import { HttpClientTestingModule } from '@angular/common/http/testing';
import {
  ComponentFixture,
  discardPeriodicTasks,
  fakeAsync,
  TestBed,
  tick
} from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbProgressbarModule } from '@ng-bootstrap/ng-bootstrap';
import { ClickOutsideModule } from 'ng-click-outside';
import { ToastrModule } from 'ngx-toastr';
import { SimplebarAngularModule } from 'simplebar-angular';

import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { SettingsService } from '~/app/shared/api/settings.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { Permissions } from '~/app/shared/models/permissions';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { PrometheusNotificationService } from '~/app/shared/services/prometheus-notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NotificationsSidebarComponent } from './notifications-sidebar.component';

describe('NotificationsSidebarComponent', () => {
  let component: NotificationsSidebarComponent;
  let fixture: ComponentFixture<NotificationsSidebarComponent>;
  let prometheusUpdatePermission: string;
  let prometheusReadPermission: string;
  let prometheusCreatePermission: string;
  let configOptReadPermission: string;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      PipesModule,
      NgbProgressbarModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      NoopAnimationsModule,
      SimplebarAngularModule,
      ClickOutsideModule
    ],
    declarations: [NotificationsSidebarComponent],
    providers: [PrometheusService, SettingsService, SummaryService, NotificationService, RbdService]
  });

  beforeEach(() => {
    prometheusReadPermission = 'read';
    prometheusUpdatePermission = 'update';
    prometheusCreatePermission = 'create';
    configOptReadPermission = 'read';
    spyOn(TestBed.inject(AuthStorageService), 'getPermissions').and.callFake(
      () =>
        new Permissions({
          prometheus: [
            prometheusReadPermission,
            prometheusUpdatePermission,
            prometheusCreatePermission
          ],
          'config-opt': [configOptReadPermission]
        })
    );
    fixture = TestBed.createComponent(NotificationsSidebarComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('prometheus alert handling', () => {
    let prometheusAlertService: PrometheusAlertService;
    let prometheusNotificationService: PrometheusNotificationService;

    const expectPrometheusServicesToBeCalledTimes = (n: number) => {
      expect(prometheusNotificationService.refresh).toHaveBeenCalledTimes(n);
      expect(prometheusAlertService.refresh).toHaveBeenCalledTimes(n);
    };

    beforeEach(() => {
      spyOn(TestBed.inject(PrometheusService), 'ifAlertmanagerConfigured').and.callFake((fn) =>
        fn()
      );

      prometheusAlertService = TestBed.inject(PrometheusAlertService);
      spyOn(prometheusAlertService, 'refresh').and.stub();

      prometheusNotificationService = TestBed.inject(PrometheusNotificationService);
      spyOn(prometheusNotificationService, 'refresh').and.stub();
    });

    it('should not refresh prometheus services if not allowed', () => {
      prometheusReadPermission = '';
      configOptReadPermission = 'read';
      fixture.detectChanges();

      expectPrometheusServicesToBeCalledTimes(0);

      prometheusReadPermission = 'read';
      configOptReadPermission = '';
      fixture.detectChanges();

      expectPrometheusServicesToBeCalledTimes(0);
    });

    it('should first refresh prometheus notifications and alerts during init', () => {
      fixture.detectChanges();

      expect(prometheusAlertService.refresh).toHaveBeenCalledTimes(1);
      expectPrometheusServicesToBeCalledTimes(1);
    });

    it('should refresh prometheus services every 5s', fakeAsync(() => {
      fixture.detectChanges();

      expectPrometheusServicesToBeCalledTimes(1);
      tick(5000);
      expectPrometheusServicesToBeCalledTimes(2);
      tick(15000);
      expectPrometheusServicesToBeCalledTimes(5);
      component.ngOnDestroy();
    }));
  });

  describe('Running Tasks', () => {
    let summaryService: SummaryService;

    beforeEach(() => {
      fixture.detectChanges();
      summaryService = TestBed.inject(SummaryService);

      spyOn(component, '_handleTasks').and.callThrough();
    });

    it('should handle executing tasks', () => {
      const running_tasks = new ExecutingTask('rbd/delete', {
        image_spec: 'somePool/someImage'
      });

      summaryService['summaryDataSource'].next({ executing_tasks: [running_tasks] });

      expect(component._handleTasks).toHaveBeenCalled();
      expect(component.executingTasks.length).toBe(1);
      expect(component.executingTasks[0].description).toBe(`Deleting RBD 'somePool/someImage'`);
    });
  });

  describe('Notifications', () => {
    it('should fetch latest notifications', fakeAsync(() => {
      const notificationService: NotificationService = TestBed.inject(NotificationService);
      fixture.detectChanges();

      expect(component.notifications.length).toBe(0);

      notificationService.show(NotificationType.success, 'Sample title', 'Sample message');
      tick(6000);
      expect(component.notifications.length).toBe(1);
      expect(component.notifications[0].title).toBe('Sample title');
      discardPeriodicTasks();
    }));
  });

  describe('Sidebar', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      notificationService = TestBed.inject(NotificationService);
      fixture.detectChanges();
    });

    it('should always close if sidebarSubject value is true', fakeAsync(() => {
      // Closed before next value
      expect(component.isSidebarOpened).toBeFalsy();
      notificationService.sidebarSubject.next(true);
      tick();
      expect(component.isSidebarOpened).toBeFalsy();

      // Opened before next value
      component.isSidebarOpened = true;
      expect(component.isSidebarOpened).toBeTruthy();
      notificationService.sidebarSubject.next(true);
      tick();
      expect(component.isSidebarOpened).toBeFalsy();
    }));

    it('should toggle sidebar visibility if sidebarSubject value is false', () => {
      // Closed before next value
      expect(component.isSidebarOpened).toBeFalsy();
      notificationService.sidebarSubject.next(false);
      expect(component.isSidebarOpened).toBeTruthy();

      // Opened before next value
      component.isSidebarOpened = true;
      expect(component.isSidebarOpened).toBeTruthy();
      notificationService.sidebarSubject.next(false);
      expect(component.isSidebarOpened).toBeFalsy();
    });
  });
});
