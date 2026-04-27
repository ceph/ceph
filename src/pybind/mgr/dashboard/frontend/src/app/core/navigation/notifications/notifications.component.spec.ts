import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { CdNotification, CdNotificationConfig } from '~/app/shared/models/cd-notification';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NotificationsComponent } from './notifications.component';
import { BehaviorSubject } from 'rxjs';

describe('NotificationsComponent', () => {
  let component: NotificationsComponent;
  let fixture: ComponentFixture<NotificationsComponent>;
  let summaryService: SummaryService;
  let notificationService: NotificationService;
  const hasUnreadSource = new BehaviorSubject(false);

  const notificationServiceMock = {
    dataSource: new BehaviorSubject([]),
    hasUnreadSource,
    get hasUnread$() {
      return hasUnreadSource.asObservable();
    },
    muteState$: new BehaviorSubject(false)
  };

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), RouterTestingModule],
    declarations: [NotificationsComponent],
    providers: [{ provide: NotificationService, useValue: notificationServiceMock }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationsComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.inject(SummaryService);
    notificationService = TestBed.inject(NotificationService);

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should subscribe and check if there are running tasks', () => {
    expect(component.hasRunningTasks).toBeFalsy();

    const task = new ExecutingTask('task', { name: 'name' });
    summaryService['summaryDataSource'].next({ executing_tasks: [task] });

    expect(component.hasRunningTasks).toBeTruthy();
  });

  it('should show notificationNew icon if there are notifications', () => {
    const notification = new CdNotification(new CdNotificationConfig());
    notificationService['dataSource'].next([notification]);
    notificationService['hasUnreadSource'].next(true);

    fixture.detectChanges();

    const icon = fixture.debugElement.nativeElement.querySelector('cd-icon');
    expect(icon).toBeTruthy();
    expect(icon.getAttribute('type')).toBe('notificationNew');
  });
});
