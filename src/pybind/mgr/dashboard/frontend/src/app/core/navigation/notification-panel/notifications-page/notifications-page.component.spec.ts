import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { BehaviorSubject } from 'rxjs';
import {
  IconModule,
  SearchModule,
  StructuredListModule,
  TagModule
} from 'carbon-components-angular';

import { NotificationsPageComponent } from './notifications-page.component';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CdNotification } from '~/app/shared/models/cd-notification';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { SharedModule } from '~/app/shared/shared.module';

describe('NotificationsPageComponent', () => {
  let component: NotificationsPageComponent;
  let fixture: ComponentFixture<NotificationsPageComponent>;
  let notificationService: NotificationService;
  let mockNotifications: CdNotification[];
  let dataSourceSubject: BehaviorSubject<CdNotification[]>;

  // Mock notification service
  const createMockNotificationService = () => {
    dataSourceSubject = new BehaviorSubject<CdNotification[]>([]);
    return {
      data$: dataSourceSubject.asObservable(),
      dataSource: dataSourceSubject,
      remove: jasmine.createSpy('remove')
    };
  };

  beforeEach(async () => {
    mockNotifications = [
      {
        title: 'Success Notification',
        message: 'Operation completed successfully',
        timestamp: new Date().toISOString(),
        type: NotificationType.success,
        application: 'TestApp'
      },
      {
        title: 'Error Notification',
        message: 'An error occurred',
        timestamp: new Date(Date.now() - 86400000).toISOString(), // Yesterday
        type: NotificationType.error,
        application: 'TestApp'
      },
      {
        title: 'Info Notification',
        message: 'System update available',
        timestamp: new Date(Date.now() - 172800000).toISOString(), // 2 days ago
        type: NotificationType.info,
        application: 'Updates'
      }
    ];

    await TestBed.configureTestingModule({
      imports: [
        FormsModule,
        SharedModule,
        IconModule,
        SearchModule,
        StructuredListModule,
        TagModule
      ],
      declarations: [NotificationsPageComponent],
      providers: [{ provide: NotificationService, useFactory: createMockNotificationService }]
    }).compileComponents();

    notificationService = TestBed.inject(NotificationService);
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationsPageComponent);
    component = fixture.componentInstance;
    dataSourceSubject.next(mockNotifications);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load notifications on init', () => {
    expect(component.notifications).toEqual(mockNotifications);
    expect(component.filteredNotifications).toEqual(mockNotifications);
  });

  it('should select notification when clicked', () => {
    const notification = mockNotifications[0];
    component.onNotificationSelect(notification);
    expect(component.selectedNotification).toBe(notification);
  });

  describe('search functionality', () => {
    it('should filter notifications by title', () => {
      component.onSearch('Success');
      expect(component.filteredNotifications.length).toBe(1);
      expect(component.filteredNotifications[0].title).toBe('Success Notification');
    });

    it('should filter notifications by message', () => {
      component.onSearch('error');
      expect(component.filteredNotifications.length).toBe(1);
      expect(component.filteredNotifications[0].title).toBe('Error Notification');
    });

    it('should filter notifications by application', () => {
      component.onSearch('Updates');
      expect(component.filteredNotifications.length).toBe(1);
      expect(component.filteredNotifications[0].application).toBe('Updates');
    });

    it('should show all notifications when search is cleared', () => {
      component.onSearch('');
      expect(component.filteredNotifications).toEqual(mockNotifications);
    });

    it('should be case insensitive', () => {
      component.onSearch('SUCCESS');
      expect(component.filteredNotifications.length).toBe(1);
      expect(component.filteredNotifications[0].title).toBe('Success Notification');
    });
  });

  describe('notification removal', () => {
    it('should remove notification', () => {
      const notification = mockNotifications[0];
      const mockEvent = {
        stopPropagation: jasmine.createSpy('stopPropagation'),
        preventDefault: jasmine.createSpy('preventDefault')
      };

      // Set up the dataSource with notifications
      dataSourceSubject.next(mockNotifications);
      fixture.detectChanges();

      component.removeNotification(notification, mockEvent as any);

      expect(mockEvent.stopPropagation).toHaveBeenCalled();
      expect(mockEvent.preventDefault).toHaveBeenCalled();
      expect(notificationService.remove).toHaveBeenCalledWith(0); // Should be called with index 0
    });

    it('should clear selection if removed notification was selected', () => {
      const notification = mockNotifications[0];
      component.selectedNotification = notification;
      const mockEvent = {
        stopPropagation: jasmine.createSpy('stopPropagation'),
        preventDefault: jasmine.createSpy('preventDefault')
      };

      // Set up the dataSource with notifications
      dataSourceSubject.next(mockNotifications);
      fixture.detectChanges();

      component.removeNotification(notification, mockEvent as any);

      expect(component.selectedNotification).toBeNull();
    });
  });

  describe('icon handling', () => {
    it('should return correct Carbon icon for success', () => {
      expect(component.getCarbonIcon(NotificationType.success)).toBe('checkmark--filled');
    });

    it('should return correct Carbon icon for error', () => {
      expect(component.getCarbonIcon(NotificationType.error)).toBe('error--filled');
    });

    it('should return correct Carbon icon for info', () => {
      expect(component.getCarbonIcon(NotificationType.info)).toBe('information--filled');
    });

    it('should return correct Carbon icon for warning', () => {
      expect(component.getCarbonIcon(NotificationType.warning)).toBe('warning--filled');
    });

    it('should return default icon for unknown type', () => {
      expect(component.getCarbonIcon(-1)).toBe('notification--filled');
    });
  });

  describe('icon color classes', () => {
    it('should return correct class for success', () => {
      expect(component.getIconColorClass(NotificationType.success)).toBe('icon-success');
    });

    it('should return correct class for error', () => {
      expect(component.getIconColorClass(NotificationType.error)).toBe('icon-error');
    });

    it('should return correct class for info', () => {
      expect(component.getIconColorClass(NotificationType.info)).toBe('icon-info');
    });

    it('should return correct class for warning', () => {
      expect(component.getIconColorClass(NotificationType.warning)).toBe('icon-warning');
    });

    it('should return empty string for unknown type', () => {
      expect(component.getIconColorClass(-1)).toBe('');
    });
  });

  describe('date formatting', () => {
    it('should format today\'s date as "Today"', () => {
      const today = new Date().toISOString();
      expect(component.formatDate(today)).toBe('Today');
    });

    it('should format yesterday\'s date as "Yesterday"', () => {
      const yesterday = new Date(Date.now() - 86400000).toISOString();
      expect(component.formatDate(yesterday)).toBe('Yesterday');
    });

    it('should format older dates in short format', () => {
      const oldDate = new Date('2023-01-15').toISOString();
      expect(component.formatDate(oldDate)).toMatch(/[A-Z][a-z]{2} \d{1,2}/);
    });
  });

  describe('time formatting', () => {
    it('should format time in 12-hour format', () => {
      const date = new Date('2023-01-15T15:30:00').toISOString();
      const formattedTime = component.formatTime(date);
      expect(formattedTime).toMatch(/\d{1,2}:\d{2} [AP]M/);
    });
  });

  it('should unsubscribe on destroy', () => {
    const unsubscribeSpy = spyOn(component['sub'], 'unsubscribe');
    component.ngOnDestroy();
    expect(unsubscribeSpy).toHaveBeenCalled();
  });
});
