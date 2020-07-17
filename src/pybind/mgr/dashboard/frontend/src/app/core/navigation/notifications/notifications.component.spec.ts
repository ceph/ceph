import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { CdNotification, CdNotificationConfig } from '../../../shared/models/cd-notification';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { NotificationService } from '../../../shared/services/notification.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { SharedModule } from '../../../shared/shared.module';
import { NotificationsComponent } from './notifications.component';

describe('NotificationsComponent', () => {
  let component: NotificationsComponent;
  let fixture: ComponentFixture<NotificationsComponent>;
  let summaryService: SummaryService;
  let notificationService: NotificationService;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), RouterTestingModule],
    declarations: [NotificationsComponent]
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

  it('should create a dot if there are running notifications', () => {
    const notification = new CdNotification(new CdNotificationConfig());
    const recent = notificationService['dataSource'].getValue();
    recent.push(notification);
    notificationService['dataSource'].next(recent);
    expect(component.hasNotifications).toBeTruthy();
    fixture.detectChanges();
    const dot = fixture.debugElement.nativeElement.querySelector('.dot');
    expect(dot).not.toBe('');
  });
});
