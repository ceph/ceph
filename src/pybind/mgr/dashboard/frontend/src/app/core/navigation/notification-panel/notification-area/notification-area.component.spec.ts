import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { BehaviorSubject } from 'rxjs';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

import { NotificationAreaComponent } from './notification-area.component';
import { NotificationService } from '../../../../shared/services/notification.service';
import { CdNotification, CdNotificationConfig } from '../../../../shared/models/cd-notification';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { SharedModule } from '../../../../shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('NotificationAreaComponent', () => {
  let component: NotificationAreaComponent;
  let fixture: ComponentFixture<NotificationAreaComponent>;
  let notificationService: any;
  let mockDataSource: BehaviorSubject<CdNotification[]>;

  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);

  const createNotification = (
    type: NotificationType,
    title: string,
    timestamp: string
  ): CdNotification => {
    const config = new CdNotificationConfig(type, title, 'message');
    const notification = new CdNotification(config);
    notification.timestamp = timestamp;
    return notification;
  };

  const mockNotifications: CdNotification[] = [
    createNotification(NotificationType.success, 'Success Today', today.toISOString()),
    createNotification(NotificationType.error, 'Error Yesterday', yesterday.toISOString())
  ];

  configureTestBed({
    imports: [SharedModule, NoopAnimationsModule],
    declarations: [NotificationAreaComponent]
  });

  beforeEach(() => {
    mockDataSource = new BehaviorSubject<CdNotification[]>(mockNotifications);
    const spy = {
      remove: jasmine.createSpy('remove'),
      dataSource: mockDataSource,
      data$: mockDataSource.asObservable()
    };

    TestBed.overrideProvider(NotificationService, { useValue: spy });
    fixture = TestBed.createComponent(NotificationAreaComponent);
    component = fixture.componentInstance;
    notificationService = TestBed.inject(NotificationService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should separate notifications into today and previous', () => {
    expect(component.todayNotifications.length).toBe(1);
    expect(component.previousNotifications.length).toBe(1);
    expect(component.todayNotifications[0].title).toBe('Success Today');
    expect(component.previousNotifications[0].title).toBe('Error Yesterday');
  });

  it('should display empty state when no notifications exist', () => {
    mockDataSource.next([]);
    fixture.detectChanges();

    const emptyElement = fixture.debugElement.query(By.css('.notification-empty'));
    expect(emptyElement).toBeTruthy();
    expect(emptyElement.nativeElement.textContent).toContain('No notifications');
  });

  it('should remove notification when close button is clicked', () => {
    const notification = mockNotifications[0];
    const event = new MouseEvent('click');
    spyOn(event, 'stopPropagation');
    spyOn(event, 'preventDefault');

    component.removeNotification(notification, event);

    expect(event.stopPropagation).toHaveBeenCalled();
    expect(event.preventDefault).toHaveBeenCalled();
    expect(notificationService.remove).toHaveBeenCalledWith(0);
  });

  it('should return correct Carbon icon based on notification type', () => {
    expect(component.getCarbonIcon(NotificationType.success)).toBe('checkmark--filled');
    expect(component.getCarbonIcon(NotificationType.error)).toBe('error--filled');
    expect(component.getCarbonIcon(NotificationType.info)).toBe('information--filled');
    expect(component.getCarbonIcon(NotificationType.warning)).toBe('warning--filled');
    expect(component.getCarbonIcon('unknown' as NotificationType)).toBe('notification--filled');
  });

  it('should return correct icon color class based on notification type', () => {
    expect(component.getIconColorClass(NotificationType.success)).toBe('icon-success');
    expect(component.getIconColorClass(NotificationType.error)).toBe('icon-error');
    expect(component.getIconColorClass(NotificationType.info)).toBe('icon-info');
    expect(component.getIconColorClass(NotificationType.warning)).toBe('icon-warning');
    expect(component.getIconColorClass('unknown' as NotificationType)).toBe('');
  });

  it('should unsubscribe from notification service on destroy', () => {
    const subSpy = spyOn(component['sub'], 'unsubscribe');
    component.ngOnDestroy();
    expect(subSpy).toHaveBeenCalled();
  });

  it('should render notifications with correct structure', () => {
    const notificationElements = fixture.debugElement.queryAll(By.css('.notification-item'));
    expect(notificationElements.length).toBe(2);

    const firstNotification = notificationElements[0];
    expect(
      firstNotification.query(By.css('.notification-title')).nativeElement.textContent
    ).toContain('Success Today');
    expect(
      firstNotification.query(By.css('.notification-message')).nativeElement.textContent
    ).toContain('message');

    const iconElement = firstNotification.query(By.css('.notification-icon svg'));
    expect(iconElement).toBeTruthy();

    expect(component.getCarbonIcon(NotificationType.success)).toBe('checkmark--filled');
  });

  it('should show section headings correctly', () => {
    const headings = fixture.debugElement.queryAll(By.css('.notification-section-heading'));
    expect(headings.length).toBe(2);
    expect(headings[0].nativeElement.textContent).toContain('Today');
    expect(headings[1].nativeElement.textContent).toContain('Previous');
  });
});
