import { ComponentFixture, TestBed } from '@angular/core/testing';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { BehaviorSubject } from 'rxjs';

import { ExecutingTask } from '~/app/shared/models/executing-task';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NotificationsComponent } from './notifications.component';

describe('NotificationsComponent', () => {
  let component: NotificationsComponent;
  let fixture: ComponentFixture<NotificationsComponent>;
  let summaryService: SummaryService;

  const hasUnreadSource = new BehaviorSubject(false);
  const muteSource = new BehaviorSubject(false);

  const notificationServiceMock = {
    hasUnread$: hasUnreadSource.asObservable(),
    muteState$: muteSource.asObservable()
  };

  configureTestBed({
    imports: [HttpClientTestingModule],
    declarations: [NotificationsComponent],
    providers: [{ provide: NotificationService, useValue: notificationServiceMock }],
    schemas: [CUSTOM_ELEMENTS_SCHEMA]
  });

  beforeEach(() => {
    hasUnreadSource.next(false);
    muteSource.next(false);
    fixture = TestBed.createComponent(NotificationsComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.inject(SummaryService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should reflect running tasks from summary service', () => {
    expect(component.hasRunningTasks()).toBe(false);

    const task = new ExecutingTask('task', { name: 'name' });
    summaryService['summaryDataSource'].next({ executing_tasks: [task] });

    expect(component.hasRunningTasks()).toBe(true);
  });

  it('should show notificationNew icon when unread notifications exist', () => {
    hasUnreadSource.next(true);
    fixture.detectChanges();

    const icon = fixture.debugElement.nativeElement.querySelector('cd-icon');
    expect(icon).toBeTruthy();
    expect(icon.getAttribute('type')).toBe('notificationNew');
  });

  it('should show notification icon when no unread and no running tasks', () => {
    fixture.detectChanges();

    const icon = fixture.debugElement.nativeElement.querySelector('cd-icon');
    expect(icon).toBeTruthy();
    expect(icon.getAttribute('type')).toBe('notification');
  });

  it('should show notificationOff icon when muted', () => {
    muteSource.next(true);
    fixture.detectChanges();

    const icon = fixture.debugElement.nativeElement.querySelector('cd-icon');
    expect(icon).toBeTruthy();
    expect(icon.getAttribute('type')).toBe('notificationOff');
  });
});
