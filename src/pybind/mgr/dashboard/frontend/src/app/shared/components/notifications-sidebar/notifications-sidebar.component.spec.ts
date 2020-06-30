import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbProgressbarModule } from '@ng-bootstrap/ng-bootstrap';
import { ClickOutsideModule } from 'ng-click-outside';
import { ToastrModule } from 'ngx-toastr';
import { SimplebarAngularModule } from 'simplebar-angular';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { PrometheusService } from '../../api/prometheus.service';
import { RbdService } from '../../api/rbd.service';
import { SettingsService } from '../../api/settings.service';
import { NotificationType } from '../../enum/notification-type.enum';
import { ExecutingTask } from '../../models/executing-task';
import { Permissions } from '../../models/permissions';
import { PipesModule } from '../../pipes/pipes.module';
import { AuthStorageService } from '../../services/auth-storage.service';
import { NotificationService } from '../../services/notification.service';
import { PrometheusAlertService } from '../../services/prometheus-alert.service';
import { PrometheusNotificationService } from '../../services/prometheus-notification.service';
import { SummaryService } from '../../services/summary.service';
import { NotificationsSidebarComponent } from './notifications-sidebar.component';

describe('NotificationsSidebarComponent', () => {
  let component: NotificationsSidebarComponent;
  let fixture: ComponentFixture<NotificationsSidebarComponent>;

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
    providers: [
      i18nProviders,
      PrometheusService,
      SettingsService,
      SummaryService,
      NotificationService,
      RbdService
    ]
  });

  beforeEach(() => {
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
    let prometheusReadPermission: string;
    let configOptReadPermission: string;

    const expectPrometheusServicesToBeCalledTimes = (n: number) => {
      expect(prometheusNotificationService.refresh).toHaveBeenCalledTimes(n);
      expect(prometheusAlertService.refresh).toHaveBeenCalledTimes(n);
    };

    beforeEach(() => {
      prometheusReadPermission = 'read';
      configOptReadPermission = 'read';
      spyOn(TestBed.inject(AuthStorageService), 'getPermissions').and.callFake(
        () =>
          new Permissions({
            prometheus: [prometheusReadPermission],
            'config-opt': [configOptReadPermission]
          })
      );

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
