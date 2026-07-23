import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { BehaviorSubject } from 'rxjs';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

import { NotificationAreaComponent } from './notification-area.component';
import { NotificationItemComponent } from '../notification-item/notification-item.component';
import { NotificationService } from '../../../../shared/services/notification.service';
import { CdNotification, CdNotificationConfig } from '../../../../shared/models/cd-notification';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { SharedModule } from '../../../../shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { HttpClientTestingModule } from '@angular/common/http/testing';
describe('NotificationAreaComponent', () => {
  let component: NotificationAreaComponent;
  let fixture: ComponentFixture<NotificationAreaComponent>;
  let mockDataSource: BehaviorSubject<CdNotification[]>;

  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);

  configureTestBed({
    imports: [HttpClientTestingModule]
  });

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
    declarations: [NotificationAreaComponent, NotificationItemComponent]
  });

  beforeEach(() => {
    mockDataSource = new BehaviorSubject<CdNotification[]>(mockNotifications);
    const spy = {
      remove: jasmine.createSpy('remove'),
      dataSource: mockDataSource,
      data$: mockDataSource.asObservable(),
      getNotificationsSnapshot: () => mockDataSource.getValue()
    };

    TestBed.overrideProvider(NotificationService, { useValue: spy });
    fixture = TestBed.createComponent(NotificationAreaComponent);
    component = fixture.componentInstance;
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

  it('should unsubscribe from notification service on destroy', () => {
    const subSpy = spyOn(component['subs'], 'unsubscribe');
    component.ngOnDestroy();
    expect(subSpy).toHaveBeenCalled();
  });

  it('should render notifications with correct structure', () => {
    const notificationElements = fixture.debugElement.queryAll(By.css('.notification-item'));
    expect(notificationElements.length).toBe(2);

    const firstNotification = notificationElements[0];
    expect(
      firstNotification.query(By.css('.cd-notification-item__title')).nativeElement.textContent
    ).toContain('Success Today');
    expect(
      firstNotification.query(By.css('.cd-notification-item__message')).nativeElement.textContent
    ).toContain('message');

    const itemElement = firstNotification.query(By.css('cd-notification-item'));
    expect(itemElement).toBeTruthy();
  });

  it('should display notification timestamps with relative date pipe', () => {
    const timestampElements = fixture.debugElement.queryAll(
      By.css('.cd-notification-item__timestamp')
    );
    expect(timestampElements.length).toBe(2);
    expect(timestampElements[0].nativeElement.textContent).toBeTruthy();
    expect(timestampElements[1].nativeElement.textContent).toBeTruthy();
  });

  it('should render notification icons with correct types', () => {
    const items = fixture.debugElement.queryAll(By.css('cd-notification-item'));
    expect(items.length).toBe(2);

    expect(items[0].attributes['ng-reflect-type']).toBe('2');
    expect(items[1].attributes['ng-reflect-type']).toBe('0');
  });

  it('should render notification dividers between items', () => {
    const dividerElements = fixture.debugElement.queryAll(By.css('.notification-divider'));
    expect(dividerElements.length).toBe(0);
  });

  it('should render notification content with proper structure', () => {
    const contentElements = fixture.debugElement.queryAll(By.css('.cd-notification-item__content'));
    expect(contentElements.length).toBe(2);

    contentElements.forEach((content) => {
      expect(content.query(By.css('.cd-notification-item__timestamp'))).toBeTruthy();
      expect(content.query(By.css('.cd-notification-item__title'))).toBeTruthy();
      expect(content.query(By.css('.cd-notification-item__message'))).toBeTruthy();
    });
  });

  it('should render notification wrappers with proper structure', () => {
    const wrapperElements = fixture.debugElement.queryAll(By.css('.notification-wrapper'));
    expect(wrapperElements.length).toBe(2);

    wrapperElements.forEach((wrapper) => {
      expect(wrapper.query(By.css('.notification-item'))).toBeTruthy();
    });
  });

  it('should show section headings correctly', () => {
    const headings = fixture.debugElement.queryAll(By.css('.notification-section-heading'));
    expect(headings.length).toBe(2);
    expect(headings[0].nativeElement.textContent).toContain('Today');
    expect(headings[1].nativeElement.textContent).toContain('Previous');
  });

  it('should render cd-notification-item for each notification', () => {
    const items = fixture.debugElement.queryAll(By.css('cd-notification-item'));
    expect(items.length).toBe(2);
  });

  it('should handle notifications with different types', () => {
    const infoNotification = createNotification(
      NotificationType.info,
      'Info Today',
      new Date(today.getTime() - 60000).toISOString()
    );
    const warningNotification = createNotification(
      NotificationType.warning,
      'Warning Today',
      new Date(today.getTime() - 30000).toISOString()
    );

    mockDataSource.next([infoNotification, warningNotification]);
    fixture.detectChanges();

    expect(component.todayNotifications.length).toBe(2);
    expect(component.todayNotifications[0].type).toBe(NotificationType.info);
    expect(component.todayNotifications[1].type).toBe(NotificationType.warning);
  });

  it('should handle empty notifications array', () => {
    mockDataSource.next([]);
    fixture.detectChanges();

    expect(component.todayNotifications.length).toBe(0);
    expect(component.previousNotifications.length).toBe(0);

    const emptyElement = fixture.debugElement.query(By.css('.notification-empty'));
    expect(emptyElement).toBeTruthy();
  });

  it('should handle notifications with only today items', () => {
    const todayOnly = [
      createNotification(
        NotificationType.success,
        'Success 1',
        new Date(today.getTime() - 60000).toISOString()
      ),
      createNotification(
        NotificationType.info,
        'Info 1',
        new Date(today.getTime() - 30000).toISOString()
      )
    ];

    mockDataSource.next(todayOnly);
    fixture.detectChanges();

    expect(component.todayNotifications.length).toBe(2);
    expect(component.previousNotifications.length).toBe(0);

    const headings = fixture.debugElement.queryAll(By.css('.notification-section-heading'));
    expect(headings.length).toBe(1);
    expect(headings[0].nativeElement.textContent).toContain('Today');
  });

  it('should handle notifications with only previous items', () => {
    const previousOnly = [
      createNotification(
        NotificationType.error,
        'Error 1',
        new Date(yesterday.getTime() + 1000).toISOString()
      ),
      createNotification(
        NotificationType.warning,
        'Warning 1',
        new Date(yesterday.getTime() + 2000).toISOString()
      )
    ];

    mockDataSource.next(previousOnly);
    fixture.detectChanges();

    expect(component.todayNotifications.length).toBe(0);
    expect(component.previousNotifications.length).toBe(2);

    const headings = fixture.debugElement.queryAll(By.css('.notification-section-heading'));
    expect(headings.length).toBe(1);
    expect(headings[0].nativeElement.textContent).toContain('Previous');
  });
});
