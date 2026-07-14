import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BehaviorSubject, Subscription } from 'rxjs';
import { Location } from '@angular/common';
import { IconModule, GridModule, LinkModule } from 'carbon-components-angular';

import { NotificationsPageComponent } from './notifications-page.component';
import { CdNotification } from '~/app/shared/models/cd-notification';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { PrometheusNotificationService } from '~/app/shared/services/prometheus-notification.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { SharedModule } from '~/app/shared/shared.module';

describe('NotificationsPageComponent', () => {
  let component: NotificationsPageComponent;
  let fixture: ComponentFixture<NotificationsPageComponent>;
  let mockNotifications: CdNotification[];
  let dataSourceSubject: BehaviorSubject<CdNotification[]>;
  let readMapSubject: BehaviorSubject<Record<string, boolean>>;
  let notificationService: any;
  let mockLocation: any;

  const createMockNotificationService = () => {
    dataSourceSubject = new BehaviorSubject<CdNotification[]>([]);
    readMapSubject = new BehaviorSubject<Record<string, boolean>>({});
    return {
      data$: dataSourceSubject.asObservable(),
      readMap$: readMapSubject.asObservable(),
      dataSource: {
        getValue: () => dataSourceSubject.getValue(),
        next: (value: CdNotification[]) => dataSourceSubject.next(value)
      },
      remove: jasmine.createSpy('remove'),
      removeAll: jasmine.createSpy('removeAll'),
      markAsRead: jasmine.createSpy('markAsRead').and.callFake((id: string) => {
        const current = readMapSubject.getValue();
        if (!current[id]) {
          const updated = { ...current, [id]: true };
          readMapSubject.next(updated);
          localStorage.setItem('cdNotificationsRead', JSON.stringify(updated));
        }
      })
    };
  };

  const mockPrometheusAlertService = {
    refresh: jasmine.createSpy('refresh')
  };

  const mockPrometheusNotificationService = {
    refresh: jasmine.createSpy('refresh')
  };

  const mockAuthStorageService = {
    getPermissions: jasmine.createSpy('getPermissions').and.returnValue({
      prometheus: { read: false },
      configOpt: { read: false }
    })
  };

  const createMockNotification = (overrides: any): CdNotification => {
    return {
      id: overrides.id,
      title: overrides.title || '',
      message: overrides.message || '',
      application: overrides.application || '',
      timestamp: overrides.timestamp || new Date().toISOString(),
      type: overrides.type || NotificationType.info,
      textClass: '',
      iconClass: '',
      duration: 0,
      borderClass: '',
      isFinishedTask: false,
      alertSilenced: false,
      ...overrides
    } as CdNotification;
  };

  beforeEach(async () => {
    mockNotifications = [
      createMockNotification({
        id: '1',
        title: 'Success Notification',
        message: 'Operation completed successfully',
        type: NotificationType.success,
        application: 'TestApp',
        timestamp: new Date().toISOString()
      }),
      createMockNotification({
        id: '2',
        title: 'Error Notification',
        message: 'An error occurred',
        type: NotificationType.error,
        application: 'TestApp',
        timestamp: new Date(Date.now() - 86400000).toISOString()
      }),
      createMockNotification({
        id: '3',
        title: 'Info Notification',
        message: 'System update available',
        type: NotificationType.info,
        application: 'Updates',
        timestamp: new Date(Date.now() - 172800000).toISOString()
      })
    ];

    mockLocation = { back: jasmine.createSpy('back') };
    const mockNotificationService = createMockNotificationService();
    notificationService = mockNotificationService;

    localStorage.removeItem('cdNotificationsRead');

    await TestBed.configureTestingModule({
      imports: [GridModule, IconModule, LinkModule, SharedModule],
      declarations: [NotificationsPageComponent],
      providers: [
        { provide: NotificationService, useValue: mockNotificationService },
        { provide: PrometheusAlertService, useValue: mockPrometheusAlertService },
        { provide: PrometheusNotificationService, useValue: mockPrometheusNotificationService },
        { provide: AuthStorageService, useValue: mockAuthStorageService },
        { provide: Location, useValue: mockLocation }
      ],
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationsPageComponent);
    component = fixture.componentInstance;
    dataSourceSubject.next(mockNotifications);
    fixture.detectChanges();
  });

  afterEach(() => {
    if (component['interval']) {
      window.clearInterval(component['interval']);
    }
    localStorage.removeItem('cdNotificationsRead');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load notifications on init', () => {
    expect(component.notifications().length).toBe(3);
  });

  it('should select notification when clicked', () => {
    const notification = component.notifications()[0];
    component.onNotificationSelect(notification);
    expect(component.selectedNotification()).toBe(notification);
  });

  describe('back navigation', () => {
    it('should call location.back()', () => {
      component.goBack();
      expect(mockLocation.back).toHaveBeenCalled();
    });
  });

  describe('clear all', () => {
    it('should call removeAll and clear selection', () => {
      component.selectedNotificationID.set('1');
      component.clearAll();
      expect(notificationService.removeAll).toHaveBeenCalled();
      expect(component.selectedNotificationID()).toBeNull();
    });
  });

  describe('remove notification', () => {
    it('should remove a notification and stop event propagation', () => {
      const mockEvent = {
        stopPropagation: jasmine.createSpy('stopPropagation')
      } as any;
      component.removeNotification(component.notifications()[0], mockEvent);
      expect(mockEvent.stopPropagation).toHaveBeenCalled();
      expect(notificationService.remove).toHaveBeenCalledWith(0);
    });

    it('should clear selection if removed notification was selected', () => {
      component.selectedNotificationID.set('1');
      const mockEvent = { stopPropagation: jasmine.createSpy() } as any;
      component.removeNotification(component.notifications()[0], mockEvent);
      expect(component.selectedNotificationID()).toBeNull();
    });

    it('should not clear selection if a different notification was removed', () => {
      component.selectedNotificationID.set('1');
      const mockEvent = { stopPropagation: jasmine.createSpy() } as any;
      component.removeNotification(component.notifications()[1], mockEvent);
      expect(component.selectedNotificationID()).toBe('1');
    });
  });

  describe('onNotificationDeleted', () => {
    it('should clear selection if deleted notification was selected', () => {
      component.selectedNotificationID.set('1');
      component.onNotificationDeleted('1');
      expect(component.selectedNotificationID()).toBeNull();
    });

    it('should not clear selection if a different notification was deleted', () => {
      component.selectedNotificationID.set('1');
      component.onNotificationDeleted('2');
      expect(component.selectedNotificationID()).toBe('1');
    });
  });

  describe('read/unread tracking', () => {
    it('should mark all notifications as unread initially', () => {
      expect(component.readMap()[mockNotifications[0].id]).toBeFalsy();
      expect(component.readMap()[mockNotifications[1].id]).toBeFalsy();
    });

    it('should call markAsRead on the service when notification selected', () => {
      component.onNotificationSelect(component.notifications()[0]);
      expect(notificationService.markAsRead).toHaveBeenCalledWith('1');
    });

    it('should reflect read state from service readMap$', () => {
      component.onNotificationSelect(component.notifications()[0]);
      fixture.detectChanges();
      expect(component.readMap()['1']).toBe(true);
    });

    it('should persist read state to localStorage via service', () => {
      component.onNotificationSelect(component.notifications()[0]);
      const stored = JSON.parse(localStorage.getItem('cdNotificationsRead'));
      expect(stored['1']).toBe(true);
    });

    it('should reflect pre-existing read state from service readMap$', () => {
      readMapSubject.next({ '2': true });
      fixture.detectChanges();
      expect(component.readMap()['2']).toBe(true);
      expect(component.readMap()['1']).toBeFalsy();
    });
  });

  describe('displayTitle and displayPreview', () => {
    it('should compute displayTitle from title for regular notifications', () => {
      expect(component.notifications()[0].displayTitle).toBe('Success Notification');
    });

    it('should compute displayTitle from alertName for Prometheus notifications', () => {
      const promNotification = createMockNotification({
        id: '4',
        title: 'Alert',
        prometheusAlert: {
          alertName: 'HighCPU',
          status: 'firing',
          severity: 'critical',
          description: 'CPU high'
        }
      });
      dataSourceSubject.next([promNotification]);
      fixture.detectChanges();
      expect(component.notifications()[0].displayTitle).toBe('HighCPU');
    });

    it('should compute displayPreview from message for regular notifications', () => {
      expect(component.notifications()[0].displayPreview).toBe('Operation completed successfully');
    });

    it('should compute displayPreview from description for Prometheus notifications', () => {
      const promNotification = createMockNotification({
        id: '4',
        title: 'Alert',
        message: 'fallback',
        prometheusAlert: {
          alertName: 'HighCPU',
          status: 'firing',
          severity: 'critical',
          description: 'CPU is above 90%'
        }
      });
      dataSourceSubject.next([promNotification]);
      fixture.detectChanges();
      expect(component.notifications()[0].displayPreview).toBe('CPU is above 90%');
    });

    it('should return empty string when no message', () => {
      const emptyNotification = createMockNotification({ id: '5', message: '' });
      dataSourceSubject.next([emptyNotification]);
      fixture.detectChanges();
      expect(component.notifications()[0].displayPreview).toBe('');
    });
  });

  it('should set up interval for Prometheus alerts when permissions exist', () => {
    mockAuthStorageService.getPermissions.and.returnValue({
      prometheus: { read: true },
      configOpt: { read: true }
    });

    fixture = TestBed.createComponent(NotificationsPageComponent);
    component = fixture.componentInstance;
    dataSourceSubject.next(mockNotifications);
    fixture.detectChanges();

    expect(component['interval']).toBeDefined();
  });

  it('should unsubscribe on destroy', () => {
    component['sub'] = new Subscription();
    const unsubscribeSpy = spyOn(component['sub'], 'unsubscribe');

    if (!component['interval']) {
      component['interval'] = window.setInterval(() => {}, 5000);
    }
    const clearIntervalSpy = spyOn(window, 'clearInterval');

    component.ngOnDestroy();

    expect(unsubscribeSpy).toHaveBeenCalled();
    expect(clearIntervalSpy).toHaveBeenCalled();
  });
});
