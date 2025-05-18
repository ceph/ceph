import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbProgressbarModule } from '@ng-bootstrap/ng-bootstrap';
import { ClickOutsideModule } from 'ng-click-outside';
import { ToastrModule } from 'ngx-toastr';
import { SimplebarAngularModule } from 'simplebar-angular';
import { of } from 'rxjs';

import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { SettingsService } from '~/app/shared/api/settings.service';
import { SucceededActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { PrometheusNotificationService } from '~/app/shared/services/prometheus-notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { TaskMessageService } from '~/app/shared/services/task-message.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NotificationsSidebarComponent } from './notifications-sidebar.component';

describe('NotificationsSidebarComponent', () => {
  let component: NotificationsSidebarComponent;
  let fixture: ComponentFixture<NotificationsSidebarComponent>;
  let notificationService: NotificationService;
  let summaryService: SummaryService;
  let authStorageService: AuthStorageService;

  configureTestBed({
    imports: [
      FormsModule,
      NgbProgressbarModule,
      ClickOutsideModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      NoopAnimationsModule,
      SimplebarAngularModule
    ],
    declarations: [NotificationsSidebarComponent],
    providers: [
      NotificationService,
      SummaryService,
      PrometheusNotificationService,
      PrometheusAlertService,
      PrometheusService,
      SettingsService,
      RbdService,
      CdDatePipe,
      RelativeDatePipe,
      TaskMessageService,
      SucceededActionLabelsI18n
    ]
  });

  beforeEach(() => {
    authStorageService = TestBed.inject(AuthStorageService);
    spyOn(authStorageService, 'getPermissions').and.returnValue({
      prometheus: { read: true },
      configOpt: { read: true }
    });

    notificationService = TestBed.inject(NotificationService);
    summaryService = TestBed.inject(SummaryService);

    spyOn(notificationService, 'data$').and.returnValue(of([]));
    spyOn(notificationService, 'sidebarSubject').and.returnValue(of(false));
    spyOn(notificationService, 'removeAll').and.stub();
    spyOn(notificationService, 'remove').and.stub();
    spyOn(summaryService, 'subscribe').and.returnValue(of({ executing_tasks: [] }));

    spyOn(localStorage, 'getItem').and.callFake((key) => {
      if (key === 'last_task') {
        return '2020-01-01T00:00:00.000Z';
      }
      if (key === 'doNotDisturb') {
        return 'false';
      }
      return null;
    });

    spyOn(localStorage, 'setItem').and.stub();

    fixture = TestBed.createComponent(NotificationsSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize doNotDisturb from localStorage', () => {
    expect(component.doNotDisturb).toBe(false);
  });

  it('should remove all notifications when removeAll is called', () => {
    component.removeAll();
    expect(notificationService.removeAll).toHaveBeenCalled();
  });

  it('should remove a notification when remove is called', () => {
    component.remove(1);
    expect(notificationService.remove).toHaveBeenCalledWith(1);
  });

  it('should close sidebar when closeSidebar is called', () => {
    component.isSidebarOpened = true;
    component.closeSidebar();
    expect(component.isSidebarOpened).toBe(false);
  });

  it('should categorize notifications correctly', () => {
    const today = new Date();
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    // Create 5 notifications for today and 5 for previous days
    const todayNotifications = Array(5).fill(null).map(() => ({ timestamp: today.toISOString() } as any));
    const previousNotifications = Array(5).fill(null).map(() => ({ timestamp: yesterday.toISOString() } as any));
    
    component.notifications = [...todayNotifications, ...previousNotifications];

    component.categorizeNotifications();
    
    // Should show all notifications
    expect(component.todayNotifications.length).toBe(5);
    expect(component.previousNotifications.length).toBe(5);
  });

  it('should return correct notification icon class', () => {
    const errorNotification = { type: 0 } as any; // NotificationType.error
    const infoNotification = { type: 1 } as any; // NotificationType.info
    const successNotification = { type: 2 } as any; // NotificationType.success

    expect(component.getNotificationIconClass(errorNotification)).toBe('error');
    expect(component.getNotificationIconClass(infoNotification)).toBe('info');
    expect(component.getNotificationIconClass(successNotification)).toBe('success');
    expect(component.getNotificationIconClass({} as any)).toBe('');
  });

  it('should toggle doNotDisturb and update localStorage', () => {
    component.doNotDisturb = false;
    component.toggleDoNotDisturb();
    
    expect(localStorage.setItem).toHaveBeenCalledWith('doNotDisturb', 'false');
    
    component.doNotDisturb = true;
    component.toggleDoNotDisturb();
    
    expect(localStorage.setItem).toHaveBeenCalledWith('doNotDisturb', 'true');
  });

  it('should handle executing tasks', () => {
    const tasks: ExecutingTask[] = [
      { name: 'task1', metadata: { component: 'comp1' } } as ExecutingTask
    ];
    
    component._handleTasks(tasks);
    
    expect(component.executingTasks).toEqual(tasks);
  });
});
